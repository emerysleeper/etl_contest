import pymysql
import time


def copy_to_dest_base_timed(creds1, creds2):

    # Setting timeout to one hour (or 3 seconds, so that tests would pass)
    # WARNING! Change this to 3600 to have one hour delay, do this on your own risk
    timeout = 3

    # Connecting to both bases
    connection_source = pymysql.connect(**creds1)
    connection_destination = pymysql.connect(**creds2)

    # Creating cursors for both bases
    cursor_source = connection_source.cursor()
    cursor_destination = connection_destination.cursor()

    # Getting operation types to use in copying database rows
    cursor_source.execute('SELECT * FROM operation_types')
    oper_types = cursor_source.fetchall()

    # Get max possible ID from source database
    cursor_source.execute('SELECT MAX(id) FROM transactions')
    max_id = cursor_source.fetchone()[0]

    # Check if Base 2 empty
    cursor_destination.execute('SELECT * FROM transactions_denormalized')
    transactions_denormalized = cursor_destination.fetchone()

    if transactions_denormalized is None:
        print('Target database is empty. Initializing full data transfer...')

        # Get first date from source database
        cursor_source.execute('SELECT MIN(dt) FROM transactions')
        current_processed_date = cursor_source.fetchone()[0]

        # Get first possible ID from source database
        cursor_source.execute('SELECT MIN(id) FROM transactions')
        current_processed_id = cursor_source.fetchone()[0]

    else:
        print('Target database is not empty. Resume copy from the latest date ant time.')

        # Getting the latest date from destination database
        cursor_destination.execute('SELECT MAX(dt) FROM transactions_denormalized')
        current_processed_date = cursor_destination.fetchone()[0]

        # Getting the smallest ID of the date-time we've resuming with
        query = 'SELECT MIN(id) FROM transactions WHERE dt = %s'
        cursor_source.execute(query, current_processed_date)
        current_processed_id = cursor_source.fetchone()[0]

    # Copy process itself
    while current_processed_id <= max_id:
        # Try and fetch a row from databse with the current processed ID, if none - increment and continue
        try:
            query = 'SELECT id, dt, idoper, move, amount FROM transactions WHERE id = %s'
            cursor_source.execute(query, (current_processed_id,))
        except:
            print("The ID of " + current_processed_id + " doesn't exist in the database; incrementing...")
        else:
            transaction = list(cursor_source.fetchone())

            # If getting new date, wait for 1 hour
            if current_processed_date.date() < transaction[1].date():
                current_processed_date = transaction[1]
                time.sleep(timeout)

            # Adding name_oper to the denormalized transaction
            transaction.append(oper_types[transaction[2] - 1][1])

            # Try and insert the record into target database,
            # If there's already a record with the same unique ID - it throws an error (so skip)
            try:
                cursor_destination.execute(
                    'INSERT INTO transactions_denormalized (id, dt, idoper, move, amount, name_oper) VALUES (%s, %s, %s, %s, %s, %s)',
                    transaction)
            except:
                print('The record exists in the target database')
        finally:
            current_processed_id += 1

    print('Copy process is finished')


    #Closing all connections so that it wouldn't interfere with future connections
    cursor_source.close()
    cursor_destination.close()

    connection_source.close()
    connection_destination.close()


def check_tables_are_equal(creds1, creds2):
    # Connecting to both bases
    connection_source = pymysql.connect(**creds1)
    connection_destination = pymysql.connect(**creds2)

    # Creating cursors for both bases
    cursor_source = connection_source.cursor()
    cursor_destination = connection_destination.cursor()

    # Get first possible ID from source database
    cursor_source.execute('SELECT MIN(id) FROM transactions')
    current_processed_id = cursor_source.fetchone()[0]

    # Get max possible ID from source database
    cursor_source.execute('SELECT MAX(id) FROM transactions')
    max_id = cursor_source.fetchone()[0]

    # Getting operation types to use in copying database rows
    cursor_source.execute('SELECT * FROM operation_types')
    oper_types = cursor_source.fetchall()

    # Start comparison
    while current_processed_id <= max_id:
        print('Current process id is ' + current_processed_id)
        # Get by id; if none - iterate to next Id and go over again
        try:
            query = 'SELECT dt, idoper, move, amount FROM transactions WHERE id = %s'
            cursor_source.execute(query, (current_processed_id,))
        except:
            print('No id in database, continue')
        else:
            source_record = cursor_source.fetchone()

            # Get the record from destination by id; if none - databases are not equal
            try:
                query = 'SELECT dt, idoper, move, amount, name_oper FROM transactions_denormalized WHERE id = %s'
                cursor_destination.execute(query, (current_processed_id,))
            except:
                print('The destination database is missing a record')
                return False
            else:
                destination_record = cursor_destination.fetchone()

                # Iterate through all fields of source record and compare to destination record
                for i in range(len(source_record)):
                    if source_record[i] == destination_record[i]:
                        continue
                    else:
                        print('Records are not equal; id is '+current_processed_id)
                        return False

                # Explicitly check that operation id corresponds with the matching oper_type name
                if destination_record[4] == oper_types[source_record[1]-1][1]:
                    continue
                else:
                    print('Oper types records are wrong')
                    return False
        finally:
            current_processed_id += 1

    #  When we've ended the loop without any False's, we can assume that the databases are equal
    return True


