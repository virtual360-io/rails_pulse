module RailsPulse
  class ApplicationJob < ActiveJob::Base
    # Rails Pulse's jobs work on its tables. In remote mode this application
    # has none, and the central summarizes and cleans up what it sends, so
    # each job is a no-op here. A central runs each job once per application,
    # in that application's database; one application's failure does not
    # keep the others from running, and is raised once they have. A job
    # whose target_application names one (a cleanup follow-up) runs in that
    # application's database only, and not at all if the central no longer
    # lists it.
    around_perform do |job, block|
      if RailsPulse::Remote.enabled?
        nil
      elsif RailsPulse::Central.enabled? && RailsPulse::Central.current_application.nil?
        application = job.target_application
        if application.nil?
          RailsPulse::Central.each_application { block.call }
        elsif RailsPulse::Central.application?(application)
          RailsPulse::Central.with_application(application.to_sym) { block.call }
        else
          RailsPulse.logger.warn "[RailsPulse] #{job.class.name} skipped: #{application} is not a configured application"
        end
      else
        block.call
      end
    end

    # The one application this job is for, or nil for every application.
    def target_application
      nil
    end
  end
end
