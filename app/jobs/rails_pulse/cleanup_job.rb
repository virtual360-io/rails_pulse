module RailsPulse
  class CleanupJob < ApplicationJob
    # A run whose only failures were stages that gave up waiting for a lock
    # is run again for that application alone, this long after it failed, up
    # to FOLLOW_UP_LIMIT times. Each follow-up starts from what the failed
    # run already removed, since every batch commits on its own.
    FOLLOW_UP_DELAY = 30.minutes
    FOLLOW_UP_LIMIT = 3

    # Performs scheduled cleanup of old RailsPulse data. A follow-up carries
    # the application it is for, the cleanup_run event it follows and how
    # many follow-ups came before it.
    # @return [Hash, nil] Stats hash with cleanup results or nil if archiving disabled
    def perform(application: nil, after_run_id: nil, follow_up: 0)
      return unless RailsPulse.configuration.archiving_enabled
      return log_superseded(after_run_id) if superseded?(after_run_id)

      log_start(follow_up)
      @previous_run_id = last_run_id
      stats = CleanupService.perform
      log_completion(stats)
      stats
    rescue StandardError => e
      log_error(e)
      schedule_follow_up(follow_up)
      raise
    end

    # The application a follow-up is for; ApplicationJob runs the job in that
    # application's database only.
    def target_application
      options = arguments.first
      options.is_a?(Hash) ? (options[:application] || options["application"]) : nil
    end

    private

    # A cleanup run recorded after the one this follow-up was scheduled for
    # (the next scheduled run, or another follow-up) has already done its
    # work, so running again would only repeat it.
    def superseded?(after_run_id)
      return false unless after_run_id && RailsPulse::Event.table_available?

      RailsPulse::CleanupRun.events.where("id > ?", after_run_id).exists?
    end

    def last_run_id
      RailsPulse::CleanupRun.events.maximum(:id) if RailsPulse::Event.table_available?
    end

    # Only the run this job just recorded counts: an older failure was
    # already followed up, or given up on, by the job that recorded it.
    def schedule_follow_up(follow_up)
      return unless RailsPulse::Event.table_available?

      run = RailsPulse::CleanupRun.latest
      return unless run && run.id != @previous_run_id && RailsPulse::CleanupRun.lock_contention_only?(run)

      if follow_up >= FOLLOW_UP_LIMIT
        RailsPulse.logger.error "[CleanupJob]#{application_label} gave up after #{follow_up} follow-up run(s) " \
          "that could not take the lock; retention is not being enforced until the next scheduled run"
        return
      end

      self.class.set(wait: FOLLOW_UP_DELAY).perform_later(
        application: RailsPulse::Central.current_application&.to_s,
        after_run_id: run.id,
        follow_up: follow_up + 1
      )
      RailsPulse.logger.warn "[CleanupJob]#{application_label} scheduled follow-up #{follow_up + 1} of " \
        "#{FOLLOW_UP_LIMIT} in #{FOLLOW_UP_DELAY.inspect}"
    rescue StandardError => e
      RailsPulse.logger.error "[CleanupJob]#{application_label} could not schedule a follow-up run: #{e.class}: #{e.message}"
    end

    def application_label
      application = RailsPulse::Central.current_application
      application ? " [#{application}]" : ""
    end

    def log_superseded(after_run_id)
      RailsPulse.logger.info "[CleanupJob]#{application_label} skipped: a cleanup ran after run #{after_run_id}"
      nil
    end

    def log_start(follow_up)
      label = follow_up.positive? ? "follow-up #{follow_up} of #{FOLLOW_UP_LIMIT}" : "scheduled cleanup"
      RailsPulse.logger.info "[CleanupJob]#{application_label} Starting #{label}"
    end

    def log_completion(stats)
      RailsPulse.logger.info "[CleanupJob]#{application_label} Cleanup completed - #{stats[:total_deleted]} records deleted"
    end

    def log_error(error)
      RailsPulse.logger.error "[CleanupJob]#{application_label} Cleanup failed: #{error.message}"
      RailsPulse.logger.error error.backtrace.join("\n")
    end
  end
end
