require "test_helper"
require "tmpdir"

module RailsPulse
  # CleanupService against a real SQLite database that a second connection
  # locks, the way ingestion and the summary job do on a central: the lock is
  # taken after the first batch commits and either freed after a few failed
  # attempts or held until every attempt is spent.
  #
  # The database is a file of its own, so the locking connection does not
  # contend with the suite's transaction and the suite's fixtures stay where
  # they are.
  class CleanupServiceLockTest < ActiveSupport::TestCase
    self.use_transactional_tests = false

    BUSY_TIMEOUT_MS = 20

    def setup
      super
      @dir = Dir.mktmpdir("rails_pulse_cleanup_lock")
      @database = File.join(@dir, "pulse.sqlite3")
      RailsPulse::ApplicationRecord.establish_connection(adapter: "sqlite3", database: @database, timeout: BUSY_TIMEOUT_MS)
      load_schema

      @locker = SQLite3::Database.new(@database)
      @locker.busy_timeout = BUSY_TIMEOUT_MS

      @original_archiving = RailsPulse.configuration.archiving_enabled
      @original_retention = RailsPulse.configuration.full_retention_period
      @original_max_records = RailsPulse.configuration.max_table_records
      RailsPulse.configuration.archiving_enabled = true
      RailsPulse.configuration.instance_variable_set(:@full_retention_period, nil)
    end

    def teardown
      @locker&.execute("ROLLBACK") if @locker&.transaction_active?
      @locker&.close
      RailsPulse::ApplicationRecord.remove_connection
      RailsPulse.configuration.archiving_enabled = @original_archiving
      RailsPulse.configuration.instance_variable_set(:@full_retention_period, @original_retention)
      RailsPulse.configuration.max_table_records = @original_max_records
      FileUtils.rm_rf(@dir) if @dir
      super
    end

    # Recovery Tests

    test "a lock freed after a few attempts is waited out and the stage finishes" do
      summarized, unsummarized = seed_operations(summarized: 7, unsummarized: 2)
      RailsPulse.configuration.max_table_records = { rails_pulse_operations: 3 }

      stats = with_lock(after_deletes: 1, release_after_failures: 2) { run_cleanup }

      assert_equal 4, stats[:count_based][:operations]
      assert_equal (summarized.last(3) + unsummarized).sort, RailsPulse::Operation.pluck(:id).sort
      assert_equal 2, @failed_deletes
      run = RailsPulse::CleanupRun.latest

      assert_equal "completed", run.outcome
      assert_equal 4, run.metadata_hash.dig("count_based", "operations")
    end

    test "a lock that never frees fails the stage with the rows it had removed and the other stages still run" do
      summarized, unsummarized = seed_operations(summarized: 7, unsummarized: 2)
      old_summary = create_overall_hourly_summary(period_end: 400.days.ago)
      RailsPulse.configuration.max_table_records = { rails_pulse_operations: 3 }

      error = assert_raises(ActiveRecord::StatementTimeout) do
        with_lock(after_deletes: 1, release_after_failures: attempts) { run_cleanup }
      end

      assert_match(/database is locked/, error.message)
      assert_equal attempts, @failed_deletes
      assert_equal summarized.drop(2) + unsummarized, RailsPulse::Operation.order(:occurred_at).pluck(:id),
        "only the first batch of two, the oldest, is gone"
      assert_not RailsPulse::Summary.exists?(old_summary.id), "later stages run once the lock is free"
      run = RailsPulse::CleanupRun.latest

      assert_equal "failed", run.outcome
      assert_equal [ "operations (count_based)" ], run.metadata_hash["failed_stages"]
      assert_equal 2, run.metadata_hash.dig("count_based", "operations")
      failure = run.metadata_hash["failures"].sole

      assert_equal "operations (count_based)", failure["stage"]
      assert_equal "ActiveRecord::StatementTimeout", failure["error"]
      assert_equal 2, failure["deleted"]
      assert failure["lock_contention"]
      assert RailsPulse::CleanupRun.lock_contention_only?(run)
    end

    test "the next run picks up where a failed one stopped and keeps unsummarized rows" do
      summarized, unsummarized = seed_operations(summarized: 7, unsummarized: 2)
      RailsPulse.configuration.max_table_records = { rails_pulse_operations: 3 }
      assert_raises(ActiveRecord::StatementTimeout) do
        with_lock(after_deletes: 1, release_after_failures: attempts) { run_cleanup }
      end

      stats = run_cleanup

      assert_equal 2, stats[:count_based][:operations], "the four over the limit, less the two already gone"
      assert_equal (summarized.last(3) + unsummarized).sort, RailsPulse::Operation.pluck(:id).sort
      assert_equal "completed", RailsPulse::CleanupRun.latest.outcome
    end

    test "batches after a lock error are half the size, so each holds the lock for less time" do
      seed_operations(summarized: 12, unsummarized: 1)
      RailsPulse.configuration.max_table_records = { rails_pulse_operations: 2 }
      limits = []
      collect = ->(*, payload) { limits << payload[:type_casted_binds].last if payload[:sql].match?(/\ADELETE FROM "rails_pulse_operations"/) && !payload[:exception] }

      stats = nil
      ActiveSupport::Notifications.subscribed(collect, "sql.active_record") do
        stub_const(CleanupService, :MIN_BATCH_SIZE, 1) do
          stats = with_lock(after_deletes: 1, release_after_failures: 1) { run_cleanup(batch_size: 4) }
        end
      end

      assert_equal 10, stats[:count_based][:operations]
      assert_equal [ 4, 2, 2, 2 ], limits
    end

    # Edge Cases

    test "a lock already held before the first batch fails the stage having removed nothing" do
      summarized, unsummarized = seed_operations(summarized: 4, unsummarized: 1)
      RailsPulse.configuration.max_table_records = { rails_pulse_operations: 1 }

      assert_raises(ActiveRecord::StatementTimeout) do
        with_lock(after_deletes: 0, release_after_failures: attempts) { run_cleanup }
      end

      assert_equal (summarized + unsummarized).sort, RailsPulse::Operation.pluck(:id).sort
      assert_equal 0, RailsPulse::CleanupRun.latest.metadata_hash["failures"].sole["deleted"]
    end

    private

    # Loading the schema file defines RailsPulse::Schema and applies it to
    # RailsPulse::ApplicationRecord's connection, reporting what it creates on
    # standard output.
    def load_schema
      stdout = $stdout
      $stdout = StringIO.new
      if defined?(RailsPulse::Schema)
        RailsPulse::Schema.call(RailsPulse::ApplicationRecord.connection)
      else
        load File.expand_path("../../../db/rails_pulse_schema.rb", __dir__)
      end
    ensure
      $stdout = stdout
    end

    def attempts
      CleanupService::LOCK_RETRY_DELAYS.size + 1
    end

    def run_cleanup(batch_size: 2)
      result = nil
      stub_const(CleanupService, :BATCH_SIZE, batch_size) do
        stub_const(CleanupService, :LOCK_RETRY_DELAYS, [ 0.001 ] * CleanupService::LOCK_RETRY_DELAYS.size) do
          stub_const(CleanupService, :SQLITE_BATCH_PAUSE, 0) { result = CleanupService.perform }
        end
      end
      result
    end

    # Takes SQLite's write lock on the second connection once `after_deletes`
    # DELETEs on operations have committed, and frees it after
    # `release_after_failures` of them have failed on it.
    def with_lock(after_deletes:, release_after_failures:)
      @failed_deletes = 0
      done = 0
      @locker.execute("BEGIN IMMEDIATE") if after_deletes.zero?
      subscriber = ActiveSupport::Notifications.subscribe("sql.active_record") do |*, payload|
        next unless payload[:sql].match?(/\ADELETE FROM "rails_pulse_operations"/)

        if payload[:exception]
          @failed_deletes += 1
          @locker.execute("ROLLBACK") if @failed_deletes == release_after_failures
        else
          done += 1
          @locker.execute("BEGIN IMMEDIATE") if done == after_deletes
        end
      end
      yield
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
    end

    # Operations older than the summarized cutoff, oldest first, and newer
    # than it; returns the ids of each.
    def seed_operations(summarized:, unsummarized:)
      create_overall_hourly_summary(period_end: 2.hours.ago)
      job = RailsPulse::Job.create!(name: "LockedCleanupJob", queue_name: "default", runs_count: 0, failures_count: 0)
      run = RailsPulse::JobRun.create!(job: job, run_id: SecureRandom.uuid, status: "success", adapter: "test", occurred_at: 1.hour.ago)
      old = summarized.downto(1).map { |hours| create_operation(run, (hours + 2).hours.ago).id }
      recent = unsummarized.downto(1).map { |minutes| create_operation(run, minutes.minutes.ago).id }
      [ old, recent ]
    end

    def create_operation(job_run, occurred_at)
      RailsPulse::Operation.create!(
        job_run: job_run, operation_type: "sql", label: "SELECT 1",
        duration: 1.0, occurred_at: occurred_at, start_time: occurred_at.to_f
      )
    end

    def create_overall_hourly_summary(period_end:)
      RailsPulse::Summary.create!(
        summarizable_type: "RailsPulse::Request", summarizable_id: 0, period_type: "hour",
        period_start: period_end.beginning_of_hour, period_end: period_end, count: 1, avg_duration: 100.0
      )
    end
  end
end
