import sqlite3
import json


def main():
    connection = sqlite3.connect('log_redo.db')
    cursor = connection.cursor()

    cursor.execute("""
        DROP TABLE IF EXISTS TP2;
    """)

    cursor.execute("""
        CREATE TABLE TP2 (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            A INTEGER,
            B INTEGER
        );
    """)
    connection.commit()

    insert_initial_values(cursor)
    connection.commit()

    print_values_in_db(cursor, 'Antes do Log REDO')

    log_redo(cursor)
    connection.commit()

    print_values_in_db(cursor, '\nDepois do Log REDO')

    connection.close()


def print_values_in_db(cursor, initial_message):
    cursor.execute("""
        SELECT A, B FROM TP2; 
    """)
    print(initial_message)
    print('(A, B)')
    for row in cursor.fetchall():
        print(f'{row}')
    print()


def get_values():
    with open('./metadado.json', 'r') as file:
        json_string = ''.join([line for line in file])
    return json.loads(json_string)


def get_initial_values():
    return get_values()['INITIAL']


def insert_initial_values(cursor):
    initial_values = get_initial_values()
    values = (initial_values['A'][0], initial_values['A'][1], initial_values['B'][0], initial_values['B'][1])
    cursor.execute("""
        INSERT INTO TP2 (A, B)
        VALUES 
            (%d, %d),
            (%d, %d);
    """ % values)


def read_log_file():
    with open('./entradaLog', 'r') as file:
        lines = [line for line in file]
    return lines


def clear_lines(lines):
    return [x.strip()[1:-1] for x in lines]


def start_transaction(new_values, line):
    new_values[line.split(' ')[-1]] = dict()


def is_in_db(column, id, value, cursor):
    cursor.execute("""
        SELECT %s FROM TP2 WHERE ID = %s
    """ % (column, id))

    if value in cursor.fetchone():
        return True
    return False


def update_value_in_db(column, id, value, cursor):
    cursor.execute("""
        UPDATE TP2 SET %s=%s WHERE ID = %s
    """ % (column, str(value), id))


def recommit_transaction(new_values, line, cursor):
    for column, row_value in new_values[get_transaction_from_commit(line)].items():
        for row, value in row_value.items():
            if is_in_db(column, row, value, cursor) is False:
                update_value_in_db(column, row, value, cursor)


def clear_ckpt(ckpt_line):
    return ckpt_line.replace(')', '').split('(')[1].replace(' ', '').split(',')


def checkpointed_transactions(lines):
    transactions_to_ignore = set()

    for line in lines:
        if line.startswith('CKPT'):
            transactions_not_checkpointed = clear_ckpt(line)
            transactions_to_ignore.update(set(transactions_not_checkpointed))
    
    return transactions_to_ignore


def get_transaction(line):
    return line.split(',')[0]


def get_transaction_from_commit(line):
    return line.split(' ')[1]


def transaction_change_info(line):
    from collections import namedtuple

    line_as_list = line.split(',')
    TChange = namedtuple('TChange', ['row', 'column', 'value'])
    return TChange(line_as_list[1], line_as_list[2], line_as_list[3])


def key_exists(new_values, key):
    if key in new_values.keys():
        return True
    return False


def save_column(new_values_transaction, column):
    if key_exists(new_values_transaction, column) is False:
        new_values_transaction[column] = dict()


def save_row(new_values_transaction_column, row):
    if key_exists(new_values_transaction_column, row) is False:
        new_values_transaction_column[row] = None


def annotate_transaction_change(new_values, line):
    transaction = get_transaction(line)
    change_info = transaction_change_info(line)

    save_column(new_values[transaction], change_info.column)
    save_row(new_values[transaction][change_info.column], change_info.row)
    new_values[transaction][change_info.column][change_info.row] = change_info.value


def log_redo(cursor):
    lines = clear_lines(read_log_file())
    new_values = dict()
    not_checkpointed = checkpointed_transactions(lines)
    didnt_redo = set()

    for line in lines:
        if line.startswith('start'):
            start_transaction(new_values, line)

        elif line.startswith('T'):
            if get_transaction(line) in not_checkpointed:
                annotate_transaction_change(new_values, line)
                didnt_redo.add(get_transaction(line))

        elif line.startswith('commit'):
            transaction = get_transaction_from_commit(line)

            if transaction in not_checkpointed:
                recommit_transaction(new_values, line, cursor)
                print(f'Transação {transaction} realizou REDO!')
                didnt_redo.remove(transaction)

        elif line.startswith('crash'):
            for transaction in didnt_redo:
                print(f'Transação {transaction} não realizou REDO!')


if __name__ == '__main__':
    main()
