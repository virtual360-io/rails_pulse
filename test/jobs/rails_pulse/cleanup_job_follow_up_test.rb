require "test_helper"

module RailsPulse
  # A cleanup run that gave up waiting for a lock is run again later; any
  # other failure, or a run that something newer already replaced, is not.
  class CleanupJobFollowUpTest < ActiveJob::TestCase
    def setup
      super
      @original_archiving = RailsPulse.configuration.archiving_enabled
      @original_retention = RailsPulse.configuration.full_retention_period
      RailsPulse.configuration.archiving_enabled = true
      RailsPulse.configuration.full_retention_period = 30.days
      RailsPulse::Event.delete_all
    end

    def teardown
      RailsPulse.configuration.archiving_enabled = @original_archiving
      RailsPulse.configuration.full_retention_period = @original_retention
      super
    end

    # Scheduling Tests

    test "a run that gave up on a lock schedules a follow-up and still fails" do
      CleanupService.any_instance.stubs(:cleanup_operations_by_time).raises(ActiveRecord::StatementTimeout, "database is locked")

      freeze_time do
        assert_raises(ActiveRecord::StatementTimeout) { CleanupJob.perform_now }

        assert_enqueued_with(
          job: CleanupJob,
          args: [ { application: nil, after_run_id: CleanupRun.latest.id, follow_up: 1 } ],
          at: CleanupJob::FOLLOW_UP_DELAY.from_now
        )
      end
      assert_equal "failed", CleanupRun.latest.outcome
    end

    test "a failure that is not lock contention schedules nothing" do
      CleanupService.any_instance.stubs(:cleanup_operations_by_time).raises(ActiveRecord::StatementInvalid, "no such column")

      assert_raises(ActiveRecord::StatementInvalid) { CleanupJob.perform_now }

      assert_no_enqueued_jobs only: CleanupJob
    end

    test "a run with a lock failure and another failure schedules nothing" do
      CleanupService.any_instance.stubs(:cleanup_operations_by_time).raises(ActiveRecord::StatementTimeout, "database is locked")
      CleanupService.any_instance.stubs(:cleanup_routes_by_time).raises(ActiveRecord::StatementInvalid, "no such column")

      assert_raises(ActiveRecord::StatementTimeout) { CleanupJob.perform_now }

      assert_no_enqueued_jobs only: CleanupJob
    end

    test "the last follow-up schedules no other and says retention is not enforced" do
      previous = CleanupRun.record!(stats: { time_based: {}, count_based: {}, total_deleted: 0 }, failed_stages: [ "x" ])
      CleanupService.any_instance.stubs(:cleanup_operations_by_time).raises(ActiveRecord::StatementTimeout, "database is locked")
      errors = []
      logger = stub(info: nil, warn: nil)
      logger.stubs(:error).with { |message| errors << message }
      RailsPulse.stubs(:logger).returns(logger)

      assert_raises(ActiveRecord::StatementTimeout) do
        CleanupJob.perform_now(after_run_id: previous.id, follow_up: CleanupJob::FOLLOW_UP_LIMIT)
      end

      assert_no_enqueued_jobs only: CleanupJob
      assert(errors.any? { |message| message.include?("gave up after #{CleanupJob::FOLLOW_UP_LIMIT} follow-up run(s)") })
    end

    # Follow-up Tests

    test "a follow-up runs the cleanup when nothing has run since the failure" do
      failed = CleanupRun.record!(stats: { time_based: {}, count_based: {}, total_deleted: 0 }, failed_stages: [ "operations (count_based)" ])

      stats = CleanupJob.perform_now(after_run_id: failed.id, follow_up: 1)

      assert_kind_of Hash, stats
      assert_equal "completed", CleanupRun.latest.outcome
    end

    test "a follow-up does nothing when a cleanup ran after the failure" do
      failed = CleanupRun.record!(stats: { time_based: {}, count_based: {}, total_deleted: 0 }, failed_stages: [ "operations (count_based)" ])
      CleanupRun.record!(stats: { time_based: {}, count_based: {}, total_deleted: 0 })
      CleanupService.expects(:perform).never

      assert_nil CleanupJob.perform_now(after_run_id: failed.id, follow_up: 1)
      assert_no_enqueued_jobs only: CleanupJob
    end

    # Edge Cases

    test "an older failed run does not make a failure without a record of its own schedule a follow-up" do
      CleanupRun.record!(
        stats: { time_based: {}, count_based: {}, total_deleted: 0 },
        failed_stages: [ "operations (count_based)" ],
        failures: [ { stage: "operations (count_based)", error: "ActiveRecord::StatementTimeout", deleted: 0, lock_contention: true } ]
      )
      CleanupService.stubs(:perform).raises(ActiveRecord::StatementTimeout, "database is locked")

      assert_raises(ActiveRecord::StatementTimeout) { CleanupJob.perform_now }

      assert_no_enqueued_jobs only: CleanupJob
    end
  end
end
