import sqlite3 as lite
import csv
from datetime import datetime, date, timedelta
from decimal import Decimal
import random


'''
    If you are building the grocery database, 
        you should only touch
        ARGS, TABLE_DEFINITIONS, and params.

    If you get an error message, 
        set all three values in ARGS to True
        and then re-run the script.
'''


class ARGS:
    CREATE_PRODUCTS_TABLE = True
    POPULATE_FACTS_TABLE = True


class params:

    class group:
        db_name = 'store_team_8.db'
        price_multiplier = 1.2
        customers_low = 1020
        customers_high = 1060
        weekend_increase = 75
        maximum_items = 90
        restock_days = [1, 3, 5]
    
    class simulation:
        start_date = date(2024, 1, 1)
        end_date = date(2024, 12, 31)
 
    class debug:
        display_daily_commits = 30

    '''
        Initial Stock Counts in Cases
        Numbers are based on profiling in HW 3
        The max stock level is set to 2 for items for which I expect
            to sell less than 2 cases worth over three days.
    '''
    class MAX_STOCK_LEVELS:
        milk = 16
        cereal = 2
        baby_food = 2
        diapers = 2
        bread = 3
        peanut_butter = 5
        jelly_jam = 10
        other = 7

    '''
        The simulation probabilities that Breitzman
            told us to use from Week 2.
    '''
    class PROBS:
        MILK = 70
        MILK_CEREAL = 50
        NoMILK_CEREAL = 5
        BABYFOOD = 20
        BABYFOOD_DIAPERS = 80
        NoBABYFOOD_DIAPERS = 1
        BREAD = 50
        PEANUTBUTTER = 10
        PEANUTBUTTER_JELLYJAM = 90
        NoPEANUTBUTTER_JELLYJAM = 5
        
    
    '''
        I am assuming that all cases have 12 items.
        Allows me to give special treatment 
            to different product categories later.
    '''
    class CASE_COUNT:
        generic = 12


'''
    TABLE_DEFINITIONS is a dict as follows:
        Key - the name of the table in the database
        Value - the CREATE TABLE statement for the table
    I wrote a lot of unused table definitions that will be useful
        in a later HW.
'''
TABLE_DEFINITIONS = {
    'date' : \
            'CREATE TABLE date(' \
                    'DateKey INT, ' \
                    'PrettyDate TEXT, ' \
                    'DayNumberInMonth INT, ' \
                    'DayNumberInYear INT, ' \
                    'WeekNumberInYear INT, ' \
                    'MonthNum INT, ' \
                    'MonthTxt TEXT, ' \
                    'Quarter INT, ' \
                    'Year INT,' \
                    'FiscalYear INT, ' \
                    'isHoliday INT, ' \
                    'isWeekend INT, ' \
                    'Season TEXT' ')',

    'products': \
            'CREATE TABLE products(' \
                    'sku INT,' \
                    'product_name TEXT, ' \
                    'product_type TEXT, ' \
                    'manufacturer TEXT, ' \
                    'base_price REAL)',

    'store' : \
            'CREATE TABLE store(' \
                    'StoreKey INT, ' \
                    'StoreManager TEXT, ' \
                    'StoreStreetAddr TEXT, ' \
                    'StoreTown TEXT, ' \
                    'StoreZipCode TEXT, ' \
                    'StorePhoneNumber TEXT, ' \
                    'StoreState TEXT' ')',
    
    'sales_transactions': \
            'CREATE TABLE sales_transactions(' \
                    'date TEXT, ' \
                    'customer_number INT, ' \
                    'sku INT, ' \
                    'salesPrice REAL, ' \
                    'items_left INT, ' \
                    'cases_ordered INT)',

    'sales_customers': \
            'CREATE TABLE sales_customers(' \
                    'date VARCHAR(8), ' \
                    'customer_number INT, ' \
                    'num_items INT, ' \
                    'total FLOAT)',

    'sales_daily': \
            'CREATE TABLE sales_daily(' \
                    'date VARCHAR(8), ' \
                    'num_customers INT, ' \
                    'num_items INT, ' \
                    'total FLOAT)',

    'inventory_daily' : \
            'CREATE TABLE inventory_daily(' \
                    'DateKey INT, ' \
                    'ProductKey INT, ' \
                    'StoreKey INT, ' \
                    'NumAvailable INT, '
                    'CostToStoreItem FLOAT, ' \
                    'CostToStore FLOAT, ' \
                    'NumCasesPurchasedToDate INT)', 

    'inventory_quarterly' : \
            'CREATE TABLE inventory_quarterly(' \
                    'ProductKey INT, ' \
                    'StoreKey INT, ' \
                    'Quarter INT, ' \
                    'Year INT, ' \
                    'CasesPurchasedToDate INT, ' \
                    'CasesPurchasedThisQuarter INT, ' \
                    'CasesOnHand INT, ' \
                    'TotalCostToStoreThisQuarter FLOAT, ' \
                    'TotalSoldByStoreThisQuarter FLOAT, ' \
                    'TotalCostToStoreThisYTD FLOAT, ' \
                    'TotalSoldByStoreThisYTD FLOAT)'
}


'''
    It would be nice if the assert functions were not called
        when DEBUG.globals.run_assert is set to False so that
        the interpreter is not calling and then returning
        every single time the DEBUG functions are called
        but I would need to copy the condition checks in
        a bunch of places.
'''
class DEBUG:
    
    class globals:
        run_assert = False
        
    def assert_params(param, param_type):
        if not DEBUG.globals.run_assert:
            return
        
        assert isinstance(param, param_type), \
            "Error!" \
            f"Expected argument of {param_type}" "\n" \
            f"Got argument of {type(param)} instead." "\n" \
            f"This parameter's value is {param}" "\n"

    '''
        The caller is responsible for passing a meaningful
            message describing what their asserts intend to do. 
    '''
    def assert_expr(expr, msg):
        if not DEBUG.globals.run_assert:
            return
            
        assert isinstance(expr, bool), \
            'Error! Your expression did not actually ' '\n' \
            '\t' 'resolve to a boolean.' \
            'This is the message you passed to assert_expr(expr, msg)' '\n' \
            f'{msg}'
        
        assert expr, msg


'''
    This class provides one common point of interaction with my team's database.
    Everything that writes to the database uses this API.
'''
class db:
    
    class globals:
        con = None
        cur = None
        commit_pending = 0

    def connect():
        db.globals.con = lite.connect(rf'{params.group.db_name}')
        db.globals.cur = db.globals.con.cursor()
        print('Database Successfully Connected To')

    def build_table(name):
        DEBUG.assert_params(name, str)
        DEBUG.assert_expr( \
            name in TABLE_DEFINITIONS.keys(), \
            'This assert checks that you are calling build_table(name) ' '\n' \
            '\t' 'with a valid table name.' '\n' \
            'This assert exists because the Python SQLite library ' '\n' \
            '\t' 'disallows placeholders for DROP TABLE statements.')
        
        db.execute_sql(f'DROP TABLE IF EXISTS {name}')
        db.execute_sql(TABLE_DEFINITIONS[name])
    
    def execute_sql(sql):
        DEBUG.assert_params(sql, str)
        
        db.globals.cur.execute(sql)

    def execute_sql_values(sql, values):
        DEBUG.assert_params(sql, str)
        DEBUG.assert_params(values, tuple)

        db.globals.cur.execute(sql, values)

    def commit():
        db.globals.con.commit()
        db.commit_pending = 0

    def close():
        db.globals.con.commit()
        db.globals.con.close()
        print('Database Connection Closed')


'''
    This API is for debugging purposes.
    In my testing, something was not playing nice with the
        existing database API when I tried to print because
        I was getting errors stating that the database connection is closed.
    These functions maintain their own connection and cursor.
'''
class db_debug():
    
    def execute_sql(sql):
        DEBUG.assert_params(sql, str)

        con = lite.connect(r'store.db')
        cur = con.cursor()
        
        results = cur.execute(sql).fetchall()
        for row in results:
            print(row)

        con.close()

    def execute_sql_values(sql, values):
        DEBUG.assert_params(values, tuple)

        con = lite.connect(r'store.db')
        cur = con.cursor()
        
        results = cur.execute(sql, values)
        for row in results:
            print(row)
        
        con.close()


'''
    This class is responsible for storing information
        about each individual product.
    Each product in memory is responsible for knowing
        how much of that product is in stock.
'''
class Product:

    class MAX_STOCK_LEVELS:
        milk = params.MAX_STOCK_LEVELS.milk
        cereal = params.MAX_STOCK_LEVELS.cereal
        baby_food = params.MAX_STOCK_LEVELS.baby_food
        diapers = params.MAX_STOCK_LEVELS.diapers
        bread = params.MAX_STOCK_LEVELS.bread
        peanut_butter = params.MAX_STOCK_LEVELS.peanut_butter
        jelly_jam = params.MAX_STOCK_LEVELS.jelly_jam
        other = params.MAX_STOCK_LEVELS.other


    def __init__(self, p_name, p_type, sku, price):
        DEBUG.assert_params(p_name, str)
        DEBUG.assert_params(p_type, str)
        DEBUG.assert_params(sku, int)
        DEBUG.assert_params(price, float)
        
        self.p_name = p_name
        self.p_type = p_type
        self.sku = sku
        self.price = price
        self.stock = 0
        self.total_cases_ordered = 0

    def __str__(self):
        return f'{self.p_name} - {self.p_type} - {self.sku} - {self.price}'

    def restock(self):
        match self.p_type:
            case 'Milk':
                max_limit = 12 * Product.MAX_STOCK_LEVELS.milk
            case 'Cereal':
                max_limit = 12 * Product.MAX_STOCK_LEVELS.cereal
            case 'Baby Food':
                max_limit = 12 * Product.MAX_STOCK_LEVELS.baby_food
            case 'Diapers':
                max_limit = 12 * Product.MAX_STOCK_LEVELS.diapers
            case 'Bread':
                max_limit = 12 * Product.MAX_STOCK_LEVELS.bread
            case 'Peanut Butter':
                max_limit = 12 * Product.MAX_STOCK_LEVELS.peanut_butter
            case 'Jelly/Jam':
                max_limit = 12 * Product.MAX_STOCK_LEVELS.jelly_jam
            case _:
                max_limit = 12 * Product.MAX_STOCK_LEVELS.other

        num_items_needed = max_limit - self.stock
        num_cases_needed = (num_items_needed + 11) // 12

        self.total_cases_ordered += num_cases_needed
        self.stock += 12*(num_cases_needed)


'''
    Products that a customer can select are represented by
        Inventory.products, which is a class-level dict
        of arrays whose definition should be inferred
        by Inventory.TYPE and Inventory.reset_lists().
'''
class Inventory:
    from enum import Enum
    
    products = {}

    class TYPE(Enum):
        OTHER = 'other'
        MILK = 'milk'
        CEREAL = 'cereal'
        BABY_FOOD = 'baby food'
        DIAPERS = 'diapers'
        BREAD = 'bread'
        PEANUT_BUTTER = 'peanut butter'
        JELLY_JAM = 'jelly jam'

    '''
        Jupyter makes lists persist in memory after I run each cell.
        I delete the existing lists in order to not have the same product appear multiple times.
    '''
    def reset_lists():
        Inventory.products = {}
        for p_type in Inventory.TYPE:
            Inventory.products[p_type.value] = []
    
    def select(p_type):
        DEBUG.assert_params(p_type, Inventory.TYPE)
        
        num_products_in_type = len(Inventory.products[p_type.value])
        product_index = random.randint(0, (num_products_in_type-1))
        last_index = \
            product_index - 1 if product_index != 0 \
            else (num_products_in_type-1)

        product = Inventory.products[p_type.value][product_index]

        '''
            If a product has 0 items in stock,
                loop through the product array until you find 
                something of the same type that is in stock.
            Relies on having shuffled the lists earlier.
        '''
        while(product.stock <= 0 and product_index != last_index):   
            product_index += 1
            product_index %= num_products_in_type
            product = Inventory.products[p_type.value][product_index]

        '''
            If everything in a product category is out of stock, 
                then return None and let the caller deal with it.
        '''
        if((product_index == last_index) and (product.stock <= 0)):
            return None
        
        product.stock -= 1
        return product

    '''
        Products are populated in memory 
            at the same time the product is populated in my dimension table.
            I found the dimension table helpful when doing some profiling.
    '''
    def create_products_table():
        
        if not ARGS.CREATE_PRODUCTS_TABLE:
            print("You don't want to create the Products table")
            return
            
        db.build_table('products')
        Inventory.reset_lists()
        db.commit()
    
        csv.register_dialect('piper', delimiter='|', quoting=csv.QUOTE_NONE)
        
        with open('Products1.txt', 'r') as csvfile:
            count = 0
            
            for row in csv.DictReader(csvfile, dialect='piper'):
                sku = int(row.get('SKU'))
                product_name = row.get('Product Name')
                product_type = row.get('itemType')
                manufacturer = row.get('Manufacturer')
                base_price = row.get('BasePrice')

                price = float(Decimal(base_price.strip('$')))
                price = round(price * params.group.price_multiplier, 2)

                current_product = Product(\
                    p_name = product_name, \
                    p_type = product_type, \
                    sku = sku, 
                    price = price
                )
                
                db.execute_sql_values( \
                        sql='insert into products values (?, ?, ?, ?, ?)',\
                        values=(sku, product_name, product_type, manufacturer, base_price))

                match product_type:
                    case 'Milk':
                        Inventory.products['milk'].append(current_product)
                    case 'Cereal':
                        Inventory.products['cereal'].append(current_product)
                    case 'Baby Food':
                        Inventory.products['baby food'].append(current_product)
                    case 'Diapers':
                        Inventory.products['diapers'].append(current_product)
                    case 'Bread':
                        Inventory.products['bread'].append(current_product)
                    case 'Peanut Butter':
                        Inventory.products['peanut butter'].append(current_product)
                    case 'Jelly/Jam':
                        Inventory.products['jelly jam'].append(current_product)
                    case _:
                        Inventory.products['other'].append(current_product)
                
                count += 1
                if count % 10000 == 0:
                    db.commit()
                    print(f"Committed {count} products")
                
            db.commit()
            print(f"Committed {count} products")
    
            '''
                We want the customer to randomly select another item of the same type 
                    if the item is out of stock.
                The select() method chooses the next index and this relies on having random products.
            '''
            for product_list in Inventory.products.values():
                random.shuffle(product_list)
            print('Products in memory successfully populated.')

    def restock_milk():
        for milk_product in Inventory.products['milk']:
            milk_product.restock()

    def restock_all():
        for product_list in Inventory.products.values():
            for product in product_list:
                product.restock()
        

'''
    Everything needed for the simulator to loop through
        a full calendar year.
'''
class simulate:

    '''
        These variables are not associated with any
            single day.
    '''
    class globals:
        num_days = 0
        start_date = params.simulation.start_date
        end_date = params.simulation.end_date

    class helpers:
        def random(prob):
            DEBUG.assert_expr( \
                ((0 < prob) and (prob < 100)), \
                'Asserts that probability is a valid percent')
            
            return random.randint(1, 100) <= prob

    class params:
        customers_low = params.group.customers_low
        customers_high = params.group.customers_high
        weekend_increase = params.group.weekend_increase
        maximum_items = params.group.maximum_items
        restock_days = params.group.restock_days
    
    class PROBS:
        MILK = params.PROBS.MILK
        MILK_CEREAL = params.PROBS.MILK_CEREAL
        NoMILK_CEREAL = params.PROBS.NoMILK_CEREAL
        BABYFOOD = params.PROBS.BABYFOOD
        BABYFOOD_DIAPERS = params.PROBS.BABYFOOD_DIAPERS
        NoBABYFOOD_DIAPERS = params.PROBS.NoBABYFOOD_DIAPERS
        BREAD = params.PROBS.BREAD
        PEANUTBUTTER = params.PROBS.PEANUTBUTTER
        PEANUTBUTTER_JELLYJAM = params.PROBS.PEANUTBUTTER_JELLYJAM
        NoPEANUTBUTTER_JELLYJAM = params.PROBS.NoPEANUTBUTTER_JELLYJAM
    
    class DEBUG:
        def print_log(day):
            if (simulate.globals.num_days % params.debug.display_daily_commits == 0) \
                or (day.current_date == simulate.globals.start_date) \
                or (day.current_date == simulate.globals.end_date):
                
                print(f'{datetime.now()} - ' \
                      f'{day.date_str} - ' \
                      f'{db.commit_pending} records created and committing')

    '''
        Holds accumulators so I can populate fact tables of higher grain.
    '''
    class Day:
        def __init__(self, current_date):
            DEBUG.assert_params(current_date, date)
            
            self.current_date = current_date
            self.date_str = current_date.strftime('%Y-%m-%d')
            self.num_items = 0
            self.num_customers = 0
            self.daily_total = 0

        def save(self):
            self.daily_total = round(self.daily_total, 2)
            
            db.commit_pending += 1
            try:
                db.execute_sql_values('INSERT INTO sales_daily VALUES (?, ?, ?, ?)', 
                    (self.date_str, self.num_items, self.num_customers, self.daily_total))
            except Exception as err:
                print("Error writing to sales_daily database table.", err)

                
    class Customer:
        def __init__(self, day):
            DEBUG.assert_params(day, simulate.Day)
            
            self.date = day.date_str
            self.customer_number = day.num_customers + 1
            self.num_items = 0
            self.max_items = random.randint(1, params.group.maximum_items)
            self.running_total = 0

        def save(self, day):
            DEBUG.assert_params(day, simulate.Day)
    
            self.running_total = round(self.running_total, 2)
            
            day.num_items += self.num_items
            day.num_customers += 1
            day.daily_total += self.running_total
            
            db.commit_pending += 1
            try:
                db.execute_sql_values('INSERT INTO sales_customers VALUES (?, ?, ?, ?)', 
                    (self.date, self.customer_number, self.num_items, self.running_total))
            except Exception as err:
                print("Error writing to sales_customers database table.", err)

    def build_tables():
        for table_name in TABLE_DEFINITIONS.keys():
            db.build_table(table_name)
        db.commit()
            
    def run():
        if not ARGS.POPULATE_FACTS_TABLE:
            print("You don't want to populate the Facts table")
            return

        simulate.build_tables()
        
        current_date = simulate.globals.start_date
        while(current_date <= simulate.globals.end_date):
            simulate.simulate_one_day(current_date)
            current_date += timedelta(1)

    def simulate_one_day(current_date):
        DEBUG.assert_params(current_date, date)

        simulate.globals.num_days += 1
        if(current_date == simulate.globals.start_date):
            Inventory.restock_all()

        '''
            Milk is restocked all 7 days of the week.
            Everything else is restocked on Tuesday, Thursday, and Saturday.
        '''
        if(current_date.weekday() in simulate.params.restock_days):
            Inventory.restock_all()
        else:
            Inventory.restock_milk()
                        
        increase = 0
        if current_date.weekday() >= 5:
            increase = simulate.params.weekend_increase
    
        day = simulate.Day(current_date)
        daily_customers = random.randint(simulate.params.customers_low + increase, simulate.params.customers_high + increase)

        for customer_number in range(daily_customers):
            simulate.simulate_one_customer(day)

        day.save()
        simulate.DEBUG.print_log(day)
        db.commit()
                
    def simulate_one_customer(day):

        DEBUG.assert_params(day, simulate.Day)

        customer_data = simulate.Customer(day)
        
        if simulate.helpers.random(simulate.PROBS.MILK):
            product = Inventory.select(Inventory.TYPE.MILK)
            simulate.buy(customer_data, product)

            if (customer_data.num_items < customer_data.max_items) \
                and (simulate.helpers.random(simulate.PROBS.MILK_CEREAL)):
                
                product = Inventory.select(Inventory.TYPE.CEREAL)
                simulate.buy(customer_data, product)

        else:
            if (simulate.helpers.random(simulate.PROBS.NoMILK_CEREAL)):
                product = Inventory.select(Inventory.TYPE.CEREAL)
                simulate.buy(customer_data, product)

        
        if (customer_data.num_items >= customer_data.max_items):
            customer_data.save(day)
            return

            
        if (simulate.helpers.random(simulate.PROBS.BABYFOOD)):
            product = Inventory.select(Inventory.TYPE.BABY_FOOD)
            simulate.buy(customer_data, product)

            if (customer_data.num_items < customer_data.max_items) \
                and (simulate.helpers.random(simulate.PROBS.BABYFOOD_DIAPERS)):
                
                product = Inventory.select(Inventory.TYPE.DIAPERS)
                simulate.buy(customer_data, product)
                
        else:
            if (simulate.helpers.random(simulate.PROBS.NoBABYFOOD_DIAPERS)):
                product = Inventory.select(Inventory.TYPE.DIAPERS)
                simulate.buy(customer_data, product)


        if (customer_data.num_items >= customer_data.max_items):
            customer_data.save(day)
            return
            
        
        if (simulate.helpers.random(simulate.PROBS.BREAD)):
            product = Inventory.select(Inventory.TYPE.BREAD)
            simulate.buy(customer_data, product)

        
        if (customer_data.num_items >= customer_data.max_items):
            customer_data.save(day)
            return

        
        if (simulate.helpers.random(simulate.PROBS.PEANUTBUTTER)):
            product = Inventory.select(Inventory.TYPE.PEANUT_BUTTER)
            simulate.buy(customer_data, product)

            if (customer_data.num_items < customer_data.max_items) \
                and (simulate.helpers.random(simulate.PROBS.PEANUTBUTTER_JELLYJAM)):
            
                product = Inventory.select(Inventory.TYPE.JELLY_JAM)
                simulate.buy(customer_data, product)

        else:
            if (simulate.helpers.random(simulate.PROBS.NoPEANUTBUTTER_JELLYJAM)):
                product = Inventory.select(Inventory.TYPE.JELLY_JAM)
                simulate.buy(customer_data, product)


        if (customer_data.num_items >= customer_data.max_items):
            return

        
        while customer_data.num_items < customer_data.max_items:
            product = Inventory.select(Inventory.TYPE.OTHER)
            simulate.buy(customer_data, product)

        customer_data.save(day)

    def buy(customer, product):
        if product is None:
            '''
                In the rare case if 
                    a customer attempts to buy from a product category
                    where there is no stock, 
                    the current customer stops buying
                    and nothing is written.
            '''
            customer.max_items = customer.num_items
            return
        
        customer.num_items += 1
        customer.running_total += product.price
        db.commit_pending += 1
        try:
            db.execute_sql_values('insert into sales_transactions values (?, ?, ?, ?, ?, ?)',
                                        (customer.date,customer.customer_number,product.sku,product.price, product.stock, product.total_cases_ordered))

        except Exception as err:
            print("Error writing to sales_transactions database table", err)


def run():
    print(f'{datetime.now()} - Data Generation started.')
    start = datetime.now()
    
    db.connect()
    Inventory.create_products_table()
    simulate.run()
    db.close()

    end = datetime.now()
    print(f'{end} - Data generation ended.')

    print(f'The data generation took {end - start}')


run()