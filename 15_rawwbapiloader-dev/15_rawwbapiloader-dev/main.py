"""Main entry point for the Wildberries API Data Loader application."""
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Any, Tuple

from core.apps import WBApp
from core.db.connection import DatabaseConnectionManager
from core.db.schema import DatabaseSchemaService, DBSchema
from core.logger import setup_logging

from config.settings import SCENARIOS, BOT_NOTIFICATIONS
from config.core import WBAppConfig, CliConfig
from core.utils.exceptions import TelegramBotNotificationException
from core.utils.notifiers import TelegramBotNotificationSender


def app_init(args) -> Tuple[Dict[str, Any], DatabaseConnectionManager, int, DBSchema]:
    """Initialize the application with the given arguments.
    
    Args:
        args: Command line arguments
        
    Returns:
        Tuple containing:
            - config: Application configuration
            - db_manager: Database connection manager
            - lpid: Load process ID
            - schema_config: Database schema configuration
    """
    # Initialize application configuration
    config = WBAppConfig(
        env=args.env,
        debug=args.debug
    ).get_config()

    # Initialize database connection
    db_manager = DatabaseConnectionManager(
        db_params=config.get('db_params')
    )
    # Generate load process ID
    lpid = db_manager.generate_lpid()
    
    # Initialize schema service and get configuration
    schema_service = DatabaseSchemaService(
        db_manager=db_manager,
        scenario=args.scenario
    )
    schema_config = schema_service.get_schema_config()
    
    # Set up logging with database handler
    setup_logging(
        level=logging.DEBUG if args.debug else logging.INFO,
        db_manager=db_manager,
        lpid=lpid,
        schema_config=schema_config,
        scenario=args.scenario
    )
    
    return config, db_manager, lpid, schema_config


def log_application_start(logger, args, lpid):
    """Log application startup information."""
    logger.info(
        "Starting application...",
        extra={
            'log_data': {
                'event_code': 'APP_START',
                'data_load_scenario': args.scenario,
                'lpid': lpid
            }
        }
    )
    
    logger.info(
        "Environment: %s, Debug: %r, Scenario: %s, Scenario args: %s, Bot: %r",
        args.env, args.debug, args.scenario, args.scenario_args, args.bot,
        extra={
            'log_data': {
                'event_code': 'LOAD_CONFIG',
                'environment': args.env,
                'debug_mode': args.debug,
                'scenario': args.scenario,
                'scenario_args': args.scenario_args,
                'bot_notifications': args.bot
            }
        }
    )


def main():
    """Main entry point for the application."""
    from datetime import datetime, timezone
    
    try:
        # Capture collection start datetime at script startup
        collection_start_dttm = datetime.now(timezone.utc).isoformat()
        
        # Parse and validate command line arguments
        args = CliConfig().parse_arguments()
        
        # Initialize application components
        config, db_manager, lpid, schema_config = app_init(args)
        
        # Get logger for main module
        logger = logging.getLogger(__name__)
        
        # Log startup information
        log_application_start(logger, args, lpid)
        
        # Get the scenario configuration
        scenario = SCENARIOS.get(args.scenario)
        if not scenario:
            logger.error(f"Unknown scenario: {args.scenario}", 
                       extra={'log_data': {'event_code': 'LOAD_CONFIG_ERROR'}})
            return 1
        
        # Initialize and run the service
        service = WBApp(
            config=config,
            scenario=args.scenario,
            scenario_args=args.scenario_args,  # Pass the actual args, not just required ones
            db_manager=db_manager,
            lpid=lpid,
            collection_start_dttm=collection_start_dttm
        ).get_service_instance()

        # # Run the service
        service.run()

        logger.info(
            "Application finished successfully",
            extra={
                'log_data': {
                    'event_code': 'APP_FINISHED',
                    'data_load_scenario': args.scenario
                }
            }
        )
        return 0
        
    except Exception as e:
        data_load_scenario = args.scenario if 'args' in locals() and hasattr(args, 'scenario') else None
        lpid = lpid if 'lpid' in locals() else None

        # Log error
        logger = logging.getLogger(__name__)
        logger.error(
            f"Fatal error. Application stopped: {e}",
            extra={
                'log_data': {
                    'event_code': 'APP_FINISHED',
                    'error_message': str(e),
                    'data_load_scenario': data_load_scenario,
                    'lpid': lpid
                }
            }
        )
        # Send notification to Telegram bot
        if args.bot == True or BOT_NOTIFICATIONS == True:
            try:
                # Bot config and init
                bot_path = Path(os.getenv('TLG_BOT_PATH'))
                bot_alert_msg = f"Scenario: {data_load_scenario}, LPID: {lpid}"

                from html import escape
                #Escape special characters for HTML
                err_text = escape(str(e))
                TelegramBotNotificationSender(bot_path).send(
                    f"🚨 <b>Fatal error. Application stopped.</b>\n"
                    f"<i>{bot_alert_msg}</i>\n\n"
                    f"<code>{err_text}</code>"
                )

            except TelegramBotNotificationException as e:
                print(f"[WARNING] Failed to send Telegram notification: {e}", file=sys.stderr)

        sys.exit(1)


if __name__ == "__main__":
    main()
