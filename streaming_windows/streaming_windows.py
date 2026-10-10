from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

if __name__ == "__main__":
    # 1. Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)

    # 2. Source: bounded datagen stream with an event-time column and watermark
    t_env.execute_sql("""
        CREATE TABLE events (
            user_id INT,
            amount DOUBLE,
            ts AS LOCALTIMESTAMP,
            WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
        ) WITH (
            'connector' = 'datagen',
            'number-of-rows' = '300',
            'rows-per-second' = '20',
            'fields.user_id.min' = '1',
            'fields.user_id.max' = '3',
            'fields.amount.min' = '1',
            'fields.amount.max' = '100'
        )
    """)

    # 3. Transformation: 5-second tumbling window per user
    result_table = t_env.sql_query("""
        SELECT
            user_id,
            window_start,
            window_end,
            COUNT(*) AS cnt,
            SUM(amount) AS total_amount
        FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '5' SECOND))
        GROUP BY user_id, window_start, window_end
    """)

    # 4. Sink + 5. Execute
    result_table.execute().print()
