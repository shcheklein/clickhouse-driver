"""Benchmark buffered DB-API reads against a local ClickHouse server."""

import argparse
from time import perf_counter

from clickhouse_driver import connect


def consume(cursor, method, batch_size):
    if method == 'iterate':
        return sum(1 for _ in cursor)
    if method == 'fetchall':
        return len(cursor.fetchall())

    count = 0
    if method == 'fetchone':
        while cursor.fetchone() is not None:
            count += 1
    else:
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            count += len(rows)
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dsn', default='clickhouse://localhost/default')
    parser.add_argument('--rows', nargs='+', type=int,
                        default=[100_000, 400_000])
    parser.add_argument('--methods', nargs='+',
                        choices=['iterate', 'fetchone', 'fetchmany', 'fetchall'],
                        default=['iterate', 'fetchmany', 'fetchall'])
    parser.add_argument('--batch-size', type=int, default=1000)
    args = parser.parse_args()

    print('rows,method,query_seconds,total_seconds')
    with connect(args.dsn) as connection:
        for count in args.rows:
            for method in args.methods:
                with connection.cursor() as cursor:
                    start = perf_counter()
                    cursor.execute(
                        'SELECT number, toString(number) FROM numbers({})'
                        .format(count)
                    )
                    query_elapsed = perf_counter() - start
                    processed = consume(cursor, method, args.batch_size)
                    total_elapsed = perf_counter() - start
                    assert processed == count
                print('{},{},{:.6f},{:.6f}'.format(
                    count, method, query_elapsed, total_elapsed
                ), flush=True)


if __name__ == '__main__':
    main()
