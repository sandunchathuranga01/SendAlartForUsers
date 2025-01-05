from apscheduler.schedulers.blocking import BlockingScheduler
from RabbitMQ.Producer.ProducerCoreFunction.status_alert_manager import sendAlartToDrc


def main():
    # Create a scheduler instance
    scheduler = BlockingScheduler()

    # Schedule the sendAlartToDrc function to run daily at 15:06
    scheduler.add_job(sendAlartToDrc, 'cron', hour=10, minute=23)

    print("Scheduler is running. Press Ctrl+C to exit.")
    try:
        # Start the scheduler
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("Scheduler stopped.")

if __name__ == "__main__":
    main()