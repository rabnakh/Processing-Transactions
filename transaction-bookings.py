import psycopg2
import sys
import threading
import signal
import queue
import random
import datetime

def parse_commandline_arguement():
    s = sys.argv[1]
    arguements = s.split(';')
    filename = arguements[0].split('=')[1]
    transaction_type = arguements[1].split('=')[1] 
    threads_count = arguements[2].split('=')[1]
    return filename, transaction_type, threads_count

def read_username_password():
    with open('password.txt') as f:
        lines = [line.rstrip() for line in f]
    username = lines[0]
    pg_password = lines[1]
    return username, pg_password

def parse_file(lines):
    bookings_queue = queue.Queue()
    next(lines)
    for line in lines:
        space_split_string = line.split(',')
        if len(space_split_string) == 2:
            space_split_string[1] = space_split_string[1].replace("\n", "")
            bookings_queue.put(space_split_string)
    return bookings_queue

class Flight_Reservation:
    book_ref_list = []
    ticket_no_list = []
    count_records_updated_bookings = 0
    count_records_updated_flights = 0
    count_records_updated_ticket = 0
    count_records_updated_ticket_flights = 0
    count_failed_transactions = 0
    count_unsuccessful_transactions = 0
    count_successful_transactions = 0

    def __init__(self,bookings_queue,conn,cursor,transaction_type,threads_count):
        query_file = open('transaction-bookings.sql', 'w')
        lock = threading.Lock()
        for i in range(int(threads_count)):
            thread = threading.Thread(target=self.reservation, args=(bookings_queue,conn,cursor,transaction_type,lock,query_file))
            thread.daemon = True
            thread.start()
        try:
            bookings_queue.join()
        except KeyboardInterrupt:
            return

    def check_failed_transaction(self,cursor,flight_id):
        query_check_flight_id = "select count(*) from flights where flight_id=" + str(flight_id)
        cursor.execute(query_check_flight_id)
        count = cursor.fetchone()[0]
        if count == 1:
            return False
        else:
            return True

    def insert_book_ref(self,conn,cursor,flight_id,generated_book_ref,query_file,lock):
        self.count_records_updated_bookings += 1
        query_scheduled_departure = "select date(scheduled_departure) from flights where flight_id=" + str(flight_id) + ";"
        query_file.write(query_scheduled_departure + '\n')
        cursor.execute(query_scheduled_departure)
        scheduled_departure = cursor.fetchone()[0]
        query_insert_booking = "insert into bookings values(" + str(generated_book_ref) + ",'" + str(scheduled_departure) + "',100);"
        query_file.write(query_insert_booking + '\n')
        cursor.execute(query_insert_booking)
        conn.commit()

    def get_available_seats(self,cursor,flight_id,query_file,lock):
        query_available_seats = "select seats_available from flights where flight_id ='" + flight_id + "';"
        query_file.write(query_available_seats + '\n')
        cursor.execute(query_available_seats)
        seats = cursor.fetchone()[0]
        return seats

    def insert_ticket_no(self,cursor,conn,generated_ticket_no,generated_book_ref,passenger_id,flight_id,query_file,lock):
        self.count_records_updated_ticket += 1
        self.count_records_updated_ticket_flights += 1
        query_insert_ticketno_ticket = "insert into ticket values(" + str(generated_ticket_no) + "," + str(generated_book_ref) + "," + str(passenger_id) + ",'pass_name'" + ");"
        query_insert_ticketno_ticket_flight = "insert into ticket_flights values(" + str(generated_ticket_no) + "," + str(flight_id) + ",'Economy',100);" 
        query_file.write(query_insert_ticketno_ticket + '\n')
        query_file.write(query_insert_ticketno_ticket_flight + '\n')
        cursor.execute(query_insert_ticketno_ticket)
        cursor.execute(query_insert_ticketno_ticket_flight)
        conn.commit()
        
    def update_seats(self,conn,cursor,flight_id,query_file,lock):
        self.count_records_updated_flights += 1
        query_update_seats_available = "update flights set seats_available = seats_available - 1 where flight_id = " + flight_id + ";"
        query_update_seats_booked = "update flights set seats_booked = seats_booked + 1 where flight_id = " + flight_id + ";"
        query_file.write(query_update_seats_available + '\n')
        query_file.write(query_update_seats_booked + '\n')
        cursor.execute(query_update_seats_available)
        cursor.execute(query_update_seats_booked)
        conn.commit()

    def reservation(self,bookings_queue,conn,cursor,transaction_type,lock,query_file):
        while True:
            lock.acquire()
            booking = bookings_queue.get()
            passenger_id = booking[0]
            flight_id = booking[1]
            failed_transaction = self.check_failed_transaction(cursor,flight_id)
            if failed_transaction != True and passenger_id != 'NULL':
                while True:
                    generated_book_ref = random.randint(1,999999)
                    if generated_book_ref not in self.book_ref_list:
                        self.book_ref_list.append(generated_book_ref)
                        break
                self.insert_book_ref(conn,cursor,flight_id,generated_book_ref,query_file,lock)
                count_seats_available = self.get_available_seats(cursor,flight_id,query_file,lock)
                if count_seats_available > 0:
                    self.count_successful_transactions += 1
                    while True:
                        generated_ticket_no = random.randint(1,9999999999999)
                        if generated_ticket_no not in self.ticket_no_list:
                            self.ticket_no_list.append(generated_ticket_no)
                            break
                    self.insert_ticket_no(cursor,conn,generated_ticket_no,generated_book_ref,passenger_id,flight_id,query_file,lock)
                    if transaction_type == 'y':
                        query_start_transaction = "start transaction;"
                        query_file.write(query_start_transaction + '\n')
                        cursor.execute(query_start_transaction)
                    self.update_seats(conn,cursor,flight_id,query_file,lock)
                    if transaction_type == 'y':
                        query_commit_transaction = "commit;"
                        query_file.write(query_commit_transaction + '\n')
                        cursor.execute(query_commit_transaction)
                else:
                    self.count_unsuccessful_transactions += 1
            else:
                self.count_failed_transactions += 1
            lock.release()
            bookings_queue.task_done()

if __name__ == '__main__':
    filename,transaction_type,threads_count = parse_commandline_arguement()
    username, pg_password = read_username_password()
    try:
        conn = psycopg2.connect(database = 'COSC3380', user = username, password = pg_password)
        cursor = conn.cursor()
        lines = open(filename)
        bookings_queue = parse_file(lines)
        reserve = Flight_Reservation(bookings_queue,conn,cursor,transaction_type,threads_count)
        print('Successful transactions: ', reserve.count_successful_transactions)
        print('Unsuccessful transactions: ', reserve.count_unsuccessful_transactions)
        print('# records update for table ticket:',reserve.count_records_updated_ticket)
        print('# records update for table ticket_flights:',reserve.count_records_updated_ticket_flights)
        print('# records updates for table bookings:',reserve.count_records_updated_bookings)
        print('# records update for flights:',reserve.count_records_updated_flights)
    except Exception:
        print('Error: Unable to connect to DBMS')