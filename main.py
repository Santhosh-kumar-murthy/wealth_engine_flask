import datetime
import json
import time

from broker_libs.broker_methods import get_kite_broker, get_angel_broker, get_shoonya_broker
from controllers.data_signals_controller import DataSignalsController, get_applied_df_zerodha, get_applied_df_angel, \
    get_applied_df_shoonya
from controllers.instruments_controller import InstrumentsController
from controllers.logs_controller import LogsController
from controllers.mqtt_publisher import MqttPublisher
from controllers.positions_controller import PositionsController, get_current_price
from controllers.settings_controllers import SettingsController


def get_observable_instruments():
    instruments_controller = InstrumentsController()
    return instruments_controller.get_observable_instruments()


if __name__ == '__main__':
    select_broker = {
        1: get_kite_broker,
        2: get_angel_broker,
        3: get_shoonya_broker,
    }
    logs_controller = LogsController()
    data_signal_controller = DataSignalsController()
    observable_instruments = get_observable_instruments()
    settings_controller = SettingsController()

    get_applied_df_methods = {
        1: get_applied_df_zerodha,
        2: get_applied_df_angel,
        3: get_applied_df_shoonya
    }

    active_time_frame = settings_controller.get_time_frame_settings()['active_time_frame']
    interval_minute = int(active_time_frame.split('_')[0])
    last_executed_minute = -1

    broker_cache = None
    active_broker_id = None
    while True:
        try:
            current_hour = datetime.datetime.now().hour
            current_minute = datetime.datetime.now().minute
            if current_minute % interval_minute == 0 and current_minute != last_executed_minute:
                last_executed_minute = current_minute
                active_system_use_broker = data_signal_controller.get_active_broker()
                broker_id = active_system_use_broker['broker_id']

                if broker_id != active_broker_id:
                    config = json.loads(active_system_use_broker['broker_config_params'])
                    broker = select_broker.get(broker_id)(config)
                    broker_cache = broker
                    active_broker_id = broker_id

                positions_manager = PositionsController()
                interval = json.loads(active_system_use_broker['broker_time_frames'])[active_time_frame]
                time.sleep(4)
                for instrument in observable_instruments:
                    get_applied_df_method = get_applied_df_methods.get(broker_id)
                    if get_applied_df_method:
                        applied_df = get_applied_df_method(instrument, broker_cache, interval)
                        if current_hour == 15 and current_minute >= 15:
                            mqtt_publisher = MqttPublisher()
                            existing_positions = positions_manager.check_for_existing_position(instrument)
                            for existing_position in existing_positions:
                                exit_price = float(get_current_price(existing_position, broker_id, broker_cache))
                                entry_price = float(existing_position['position_entry_price'])
                                position_type = existing_position['position_type']
                                payload = {
                                    "intraday_exit_position": existing_position,
                                    "exit_price": exit_price,
                                    "position_type": position_type
                                }
                                mqtt_publisher.publish_payload(payload)
                        else:
                            positions_manager.analyze_to_take_position(applied_df, instrument, interval, broker_id,
                                                                       broker_cache)
                    else:
                        logs_controller.add_log(f"No method found for broker_id: {broker_id}")
        except Exception as e:
            logs_controller.add_log(f"Error occurred: {e}")
        time.sleep(60 - datetime.datetime.now().second)
