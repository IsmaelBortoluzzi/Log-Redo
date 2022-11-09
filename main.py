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
    with open('text_files/metadado.json', 'r') as file:
        json_string = ''.join([line for line in file])
    return json.loads(json_string)


def get_initial_values():
    return get_values()['INITIAL']


def insert_initial_values(cursor):
    initial_values = get_initial_values()
    values = [(initial_values['A'].pop(0), initial_values['B'].pop(0)) for x in range(len(initial_values['A']))]
    query = f"""
        INSERT INTO TP2 (A, B) 
        VALUES 
            {str(values)[1:-1]};
    """
    cursor.execute(query)


def read_log_file():
    with open('text_files/entradaLog', 'r') as file:
        lines = list(filter(lambda line: line != '\n', file))
    return lines


def clear_lines(lines):
    return [x.strip()[1:-1] for x in lines]


def start_transaction(new_values, line):
    new_values[get_transaction_from_start_or_commit(line)] = dict()


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
    for column, row_value in new_values[get_transaction_from_start_or_commit(line)].items():
        for row, value in row_value.items():
            if is_in_db(column, row, value, cursor) is False:
                update_value_in_db(column, row, value, cursor)


def clear_ckpt(ckpt_line):

    if '()' in ckpt_line:
        return list()
    try:
        return ckpt_line.replace(')', '').split('(')[1].replace(' ', '').split(',')
    except IndexError:
        return list()  # CKPT is empty (<CKPT>)


def get_transaction_from_start_or_commit(line):
    return line.split(' ')[-1]


def get_all_transactions(lines):
    return list(map(get_transaction_from_start_or_commit, filter(lambda line: line.startswith('start'), lines)))


def get_starts_after_empty_ckpt(transactions_in_ckpt, lines):
    all_transactions_to_work = set()
    lines_reversed = reversed(lines)
    index = len(lines) - 1

    for line in lines_reversed:
        if line.startswith('CKPT'):
            break

        if line.startswith('start'):
            transaction = get_transaction_from_start_or_commit(line)
            all_transactions_to_work.add(transaction)
            if transaction in transactions_in_ckpt:
                transactions_in_ckpt.remove(transaction)

        index -= 1

    return all_transactions_to_work, index + 1


def get_earliest_start(transactions_in_ckpt, lines):
    all_transactions_to_work = set()
    lines_reversed = reversed(lines)
    index = len(lines) - 1

    for line in lines_reversed:
        if len(transactions_in_ckpt) == 0:
            break

        if line.startswith('start'):
            transaction = get_transaction_from_start_or_commit(line)
            all_transactions_to_work.add(transaction)
            if transaction in transactions_in_ckpt:
                transactions_in_ckpt.remove(transaction)

        index -= 1

    return all_transactions_to_work, index + 1


def checkpointed_transactions(lines):
    lines_reversed = reversed(lines)

    for line in lines_reversed:
        if line.startswith('CKPT'):
            ckpt_transactions = clear_ckpt(line)
            if len(ckpt_transactions) == 0:
                transactions_in_ckpt, index = get_starts_after_empty_ckpt(ckpt_transactions, lines)
            else:
                transactions_in_ckpt, index = get_earliest_start(ckpt_transactions, lines)
            break
    else:
        transactions_in_ckpt = get_all_transactions(lines)
        index = 0

    return transactions_in_ckpt, index


def get_transaction(line):
    return line.split(',')[0]


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
    not_checkpointed, index = checkpointed_transactions(lines)
    didnt_redo = set()

    while index < len(lines):
        line = lines[index]

        if line.startswith('start'):
            if get_transaction_from_start_or_commit(line) in not_checkpointed:
                start_transaction(new_values, line)

        elif line.startswith('T'):
            if get_transaction(line) in not_checkpointed:
                annotate_transaction_change(new_values, line)
                didnt_redo.add(get_transaction(line))

        elif line.startswith('commit'):
            transaction = get_transaction_from_start_or_commit(line)

            if transaction in not_checkpointed:
                recommit_transaction(new_values, line, cursor)
                print(f'Transação {transaction} realizou REDO!')
                didnt_redo.remove(transaction)

        elif line.startswith('crash'):
            break

        index += 1

    for transaction in didnt_redo:
        print(f'Transação {transaction} não realizou REDO!')


if __name__ == '__main__':
    main()
