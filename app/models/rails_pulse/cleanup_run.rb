module RailsPulse
  # One CleanupService run, stored as an Event of kind cleanup_run: `value`
  # is the rows it deleted, `outcome` completed or failed, and `metadata` the
  # per-table counts, the application (on a central) and, for each stage that
  # failed, its error, the rows it had removed before failing and whether it
  # gave up waiting for a lock. rails_pulse:status and the dashboard's
  # storage-pressure signals read the latest to say whether cleanup is running
  # at all, and CleanupJob reads it to decide on a follow-up run; nothing else
  # depends on these rows, so event retention prunes them like any other kind.
  class CleanupRun
    KIND = "cleanup_run".freeze
    SUBJECT = "cleanup".freeze

    class << self
      def events
        RailsPulse::Event.of_kind(KIND)
      end

      def record!(stats:, failed_stages: [], failures: [], application: nil, ran_at: Time.current)
        RailsPulse::Event.create!(
          kind:        KIND,
          subject:     SUBJECT,
          outcome:     failed_stages.any? ? "failed" : "completed",
          value:       stats[:total_deleted].to_i,
          occurred_at: ran_at,
          metadata:    {
            application:   application&.to_s,
            time_based:    stats[:time_based],
            count_based:   stats[:count_based],
            failed_stages: failed_stages,
            failures:      failures
          }.compact.to_json
        )
      end

      def latest
        events.recent.first
      end

      # True when the run failed and every failed stage gave up waiting for a
      # lock: running again later, when the contention is over, can finish
      # the job. Any other failure needs a person.
      def lock_contention_only?(event)
        failures = event.metadata_hash["failures"].to_a
        event.outcome == "failed" && failures.any? && failures.all? { |failure| failure["lock_contention"] }
      end
    end
  end
end
