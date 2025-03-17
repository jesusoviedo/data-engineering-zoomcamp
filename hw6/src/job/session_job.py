from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration
from pyflink.table.window import Session
from pyflink.table.expressions import col, lit

def create_events_aggregated_sink(t_env):
    table_name = 'processed_events_aggregated'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            pulocationid integer,
            dolocationid integer,
            session_start timestamp(3),
            session_end timestamp(3),
            num_trips BIGINT,
            PRIMARY KEY (pulocationid, dolocationid, session_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres-hw:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_events_source_kafka(t_env):
    table_name = "events_green_trips"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime timestamp(3),
            lpep_dropoff_datetime timestamp(3),
            PULocationID integer,
            DOLocationID integer,
            passenger_count integer,
            trip_distance double,
            tip_amount double,
            WATERMARK FOR lpep_dropoff_datetime AS lpep_dropoff_datetime - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1-hw:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def log_aggregation():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(60 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(
            lambda event, timestamp: event['lpep_dropoff_datetime']
        )
    )
    try:
        source_table = create_events_source_kafka(t_env)
        aggregated_table = create_events_aggregated_sink(t_env)

        table = t_env.from_path(source_table)

        session_window = table.window(Session.with_gap(lit(5).minutes)\
                                    .on(col('lpep_dropoff_datetime'))\
                                    .alias('session'))

        result = session_window.group_by(col('PULocationID'), col('DOLocationID'), col('session'))\
                                .select(col('PULocationID').alias('pulocationid')\
                                    ,col('DOLocationID').alias('dolocationid')\
                                    ,col('session').start.alias('session_start')\
                                    ,col('session').end.alias('session_end')\
                                    ,col('PULocationID').count.alias('num_trips'))

        result.execute_insert(aggregated_table).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_aggregation()
