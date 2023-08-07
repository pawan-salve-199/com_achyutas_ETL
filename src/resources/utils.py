import random,os,json
from faker import Faker
from pyspark.sql.functions import *
from pyspark.sql.types import *

fake=Faker()

def openJson(filepath):
    """ open a json file and return a dict """
    if isinstance(filepath, str) and os.path.exists(filepath):
        with open(filepath, "r") as f:
            data = json.load(f)
        return data

#utils for DimReseller
# def subkey(arr):
#     return random.choice(arr)


# def mobile():
#     return fake.basic_phone_number()


# def name():
#     return fake.name()


# def address():
#     return fake.address()


# def types_udf(arr):
#     return random.choice(arr)


# def num_employees_udf(reseller_type):
#     if reseller_type == "Warehouse":
#         return random.randint(40, 100)

#     elif reseller_type == 'Value Added Reseller':
#         return random.randint(10, 25)

#     elif reseller_type == 'Specialty Bike Shop':
#         return random.randint(10, 25)
#     else:
#         return None
    
    
# def annual_sales():
#     return random.randint(8000000, 300000000)


# def order_udf(arr):
#     return random.choice(arr)



# def min_payment_method(arr):
#     return random.choice(arr)

# def min_payment_amount(min_payment):
#     if min_payment == 3:
#         return 600
#     elif min_payment == 2:
#         return 300
#     else:
#         return None

types = ['Value Added Reseller', 'Specialty Bike Shop', 'Warehouse']
order_freq = ['A', 'Q', 'S']
order_month = [3, 4, 5, 6, 8, 1, 12, 10, None]
random_years = [2011, 2012, 2008, 2005, 2010, 2013, 2014, 2006]
random_last_years = [2011, 2012, 2014, 2023]
product_line = ['Road', 'Mountain', 'Touring']
banks = ['Primary International', 'United Security', 'Primary Bank & Reserve', 'Guardian Bank', 'HDFC', 'DBS', 'AXIS',
            'ReserveSecurity']
min_payment = [1, 2, 3, None]
years = [1984, 1988, 1984, 1991, 1996, 1998, 1992, 2002, 2004, 2003, 1993, 1999]
array_choice_udf = udf(lambda arr: random.choice(arr), IntegerType())
array_type_udf = udf(lambda arr: random.choice(arr), StringType())
mobile_udf = udf(lambda: fake.basic_phone_number(), StringType())
reseller_names_udf = udf(lambda: fake.name(), StringType())
reseller_employees_udf = udf(lambda reseller_type: random.randint(10, 25) if reseller_type in types[1:] else None,IntegerType())
order_udf = udf(lambda arr: random.choice(arr), StringType())
order_month_udf = udf(lambda arr: random.choice(arr), IntegerType())
order_year_udf = udf(lambda order_month: random.choice(random_years) if order_month else None, IntegerType())
order_last_udf = udf(lambda order_month: random.choice(random_last_years) if order_month else None,IntegerType())
product_line_udf = udf(lambda arr: random.choice(arr), StringType())
address_udf = udf(lambda: fake.address(), StringType())
annual_sales_udf = udf(lambda: random.randint(8000000, 300000000), IntegerType())
banks_udf = udf(lambda arr: random.choice(arr), StringType())
min_payment_udf = udf(lambda arr: random.choice(arr), IntegerType())
min_amount_udf = udf(lambda min_payment: 600 if min_payment == 3 else (300 if min_payment == 2 else None),
                        IntegerType())
years_udf = udf(lambda arr: random.choice(arr), IntegerType())

# utils for DimCustomers

@udf()
def first_name():
    return fake.first_name()
@udf()
def last_name():
    return fake.last_name()
@udf()
def mail():
    return fake.free_email()
@udf()
def BirthDate():
    return fake.passport_dob().strftime("%Y-%m-%d")
@udf()
def mobile():
    return fake.basic_phone_number()
@udf()
def street_address():
    return fake.street_address()


#=======================================================================
@udf()
def IP():
    return fake.ipv4()

prod_sub_cat_data=[('Microsoft', 'Microsoft', 'Microsoft'), ('AWS', 'Amazon web services', 'Amazon web services'),
                    ('Azure', 'Azure', 'Azure'),
                    ('Python', 'Python', 'Python'), ('Java', 'Java', 'Java'),
                    ('Visual studio', 'Visual studio', 'Visual studio'),
                    ('SQL', 'SQL', 'SQL'), ('Google chrome', 'chrome', 'chrome'),
                    ('Power BI', 'Power BI', 'médicaments'),
                    ('Adobe photoshop', 'photoshop', 'photoshop'),
                    ('cpu', 'upc', 'cpu'), ('gpu', 'gpu', 'gpu'), ('ram', 'ram', 'ram'), ('ssd', 'ssd', 'ssd'),
                    ('hdd', 'hdd', 'hdd'),
                    ('smartphones', 'telefono', 'telephone'),
                    ('Tablets', 'tableta', 'tablette'),
                    ('smartwatches', 'relojes', 'montres intelligents'),
                    ('Tv', 'Televisor', 'latele'),
                    ('Headphone', 'auricular', 'casquede'),
                    ('AC', 'CA', 'CA'),
                    ('refrigerators', 'refrigerador', 'refrigerateur'),
                    ('Fiction', 'ficcion', 'ficcion'),
                    ('Non-fiction', 'no-ficcion', 'no-fiction'),
                    ('reference', 'referencia', 'reference'),
                    ('educational', 'educative', 'educative'),
                    ('childrens', 'ninos', 'pour enfants'),
                    ('spirituality', 'espiritualided', 'spiritualite'),
                    ('poetry', 'poesia', 'poesie'),
                    ('cookbooks', 'libros de cocina', 'livers de coisine'),
                    ('Health', 'salud y bienesta', 'sante'),
                    ('organic chemical', 'quimicos', 'produitos'),
                    ('inorganic chemical', 'in quimicos', 'produitos in'),
                    ('polymers', 'polimeros', 'polymeres'),
                    ('agrochemicals', 'agroquimicos', 'agrochimiques'),
                    ('food additives', 'aditivos', 'additif'),
                    ('Bakery', 'de panaderia', 'boulamgerie'),
                    ('Dairy', 'lacteos', 'laitier'),
                    ('meat&poultry', 'corne y corral', 'viande et volaille'),
                    ('seafood', 'mariscos', 'fruit de mer'),
                    ('fruits&vegetables', 'frutas y vegetables', 'fruits et legumes'),
                    ('cereals&grains', 'cereals y granos', 'cereals et grains'),
                    ('snack', 'bocadillo', 'collation'),
                    ('Beverages', 'bebidas', 'breuvages'),
                    ('networking equipment', 'equipo de red', 'equipment demise'),
                    ('laptops', 'portatil', 'portable'),
                    ('computers', 'computadoras', 'des ordinateurs'),
                    ('Internet service', 'servicio de internet', 'service internet'),
                    ('IOT', 'internet delas cosas', 'pas'),
                    ('ARVR', 'ARVR', 'ARVR'),
                    ('Aircraft', 'Aeronave', 'Avion'),
                    ('spacecraft', 'naves espaciales', 'vaisseau spatial'),
                    ('RocketSystem', 'sistema de', 'rocket system'),
                    ('Aerospaces materials', 'materials aero', 'materiaor aerospatiaur'),
                    ('Aircraftcomponents', 'componentes de aeronaves', 'composants davion'),
                    ('Drones', 'Drones', 'Drones'),
                    ('medications', 'medicamentos', 'medicaments'),
                    ('vaccines', 'vacunas', 'vaccins'),
                    ('cns medications', 'medicamentos del snc', 'medicament du snc'),
                    ('Antiviral medications', 'medicamentos antivirales', 'medicament antiviral'),
                    ('Hormonal', 'medicamentos hormonales', 'medicaments hormonaux')
                    ]

bridge_data= [(5, 'Software product', 'MicrosoftTools', 'MicrosoftTools', 'MicrosoftTools'),
            (5, 'Software product', 'AWS', 'Amazon web services', 'Amazon web services'),
            (5, 'Software product', 'Azure', 'Azure', 'Azure'),
            (5, 'Software product', 'Python', 'Python', 'Python'),
            (5, 'Software product', 'Java', 'Java', 'Java'),
            (5, 'Software product', 'Visual studio', 'Visual studio', 'Visual studio'),
            (5, 'Software product', 'SQL', 'SQL', 'SQL'),
            (5, 'Software product', 'GCP', 'GCP', 'GCP'),
            (5, 'Software product', 'Power BI', 'Power BI', 'Power BI'),
            (5, 'Software product', 'Adobe photoshop', 'photoshop', 'photoshop'),
            (6, 'Hardware product', 'cpu', 'upc', 'cpu'),
            (6, 'Hardware product', 'gpu', 'gpu', 'gpu'),
            (6, 'Hardware product', 'ram', 'ram', 'ram'),
            (6, 'Hardware product', 'ssd', 'ssd', 'ssd'),
            (6, 'Hardware product', 'hdd', 'hdd', 'hdd'),
            (6, 'Hardware product', 'smartphones', 'telefono', 'telephone'),
            (6, 'Hardware product', 'Tablets', 'tableta', 'tablette'),
            (6, 'Hardware product', 'smartwatches', 'relojes', 'montres intelligents'),
            (6, 'Hardware product', 'Tv', 'Televisor', 'latele'),
            (6, 'Hardware product', 'Headphone', 'auricular', 'casquede'),
            (6, 'Hardware product', 'AC', 'CA', 'CA'),
            (6, 'Hardware product', 'refrigerators', 'refrigerador', 'refrigerateur'),
            (7, 'Books', 'Fiction', 'ficcion', 'ficcion'),
            (7, 'Books', 'Non-fiction', 'no-ficcion', 'no-fiction'),
            (7, 'Books', 'reference', 'referencia', 'reference'),
            (7, 'Books', 'educational', 'educative', 'educative'),
            (7, 'Books', 'childrens', 'ninos', 'pour enfants'),
            (7, 'Books', 'spirituality', 'espiritualided', 'spiritualite'),
            (7, 'Books', 'poetry', 'poesia', 'poesie'),
            (7, 'Books', 'cookbooks', 'libros de cocina', 'livers de coisine'),
            (7, 'Books', 'Health', 'salud y bienesta', 'sante'),
            (8, 'chemicals', 'organic chemical', 'quimicos', 'produitos'),
            (8, 'chemicals', 'inorganic chemical', 'in quimicos', 'produitos in'),
            (8, 'chemicals', 'polymers', 'polimeros', 'polymeres'),
            (8, 'chemicals', 'agrochemicals', 'agroquimicos', 'agrochimiques'),
            (8, 'chemicals', 'food additives', 'aditivos', 'additif'),
            (9, 'Food and Beverages', 'Bakery', 'de panaderia', 'boulamgerie'),
            (9, 'Food and Beverages', 'Dairy', 'lacteos', 'laitier'),
            (9, 'Food and Beverages', 'meat&poultry', 'corne y corral', 'viande et volaille'),
            (9, 'Food and Beverages', 'seafood', 'mariscos', 'fruit de mer'),
            (9, 'Food and Beverages', 'fruits&vegetables', 'frutas y vegetables', 'fruits et legumes'),
            (9, 'Food and Beverages', 'cereals&grains', 'cereals y granos', 'cereals et grains'),
            (9, 'Food and Beverages', 'snack', 'bocadillo', 'collation'),
            (9, 'Food and Beverages', 'Beverages', 'bebidas', 'breuvages'),
            (10, 'Telecommunication', 'networking equipment', 'equipo de red', 'equipment demise'),
            (10, 'Telecommunication', 'laptops', 'portatil', 'portable'),
            (10, 'Telecommunication', 'computers', 'computadoras', 'des ordinateurs'),
            (10, 'Telecommunication', 'Internet service', 'servicio de internet', 'service internet'),
            (10, 'Telecommunication', 'IOT', 'internet delas cosas', 'pas'),
            (10, 'Telecommunication', 'ARVR', 'ARVR', 'ARVR'),
            (11, 'Aerospaces', 'Aircraft', 'Aeronave', 'Avion'),
            (11, 'Aerospaces', 'spacecraft', 'naves espaciales', 'vaisseau spatial'),
            (11, 'Aerospaces', 'RocketSystem', 'sistema de', 'rocket system'),
            (11, 'Aerospaces', 'Aerospaces materials', 'materials aero', 'materiaor aerospatiaur'),
            (11, 'Aerospaces', 'Aircraftcomponents', 'componentes de aeronaves', 'composants davion'),
            (11, 'Aerospaces', 'Drones', 'Drones', 'Drones'),
            (12, 'Pharmaceuticals', 'medications', 'medicamentos', 'medicaments'),
            (12, 'Pharmaceuticals', 'vaccines', 'vacunas', 'vaccins'),
            (12, 'Pharmaceuticals', 'cns medications', 'medicamentos del snc', 'medicament du snc'),
            (12, 'Pharmaceuticals', 'Antiviral medications', 'medicamentos antivirales', 'medicament antiviral'),
            (12, 'Pharmaceuticals', 'Hormonal', 'medicamentos hormonales', 'medicaments hormonaux')
            ]

promotion_data= [('save upto 20 % on each product', 'ahorre hasta un 20 % en cada producto',
                     "économisez jusqu'à 20 % sur chaque produit", 0.20, "Discontinued Product", "Descatalogado",
                     "Ce produit n'est plus commercialisé", "Reseller", "Distribuidor", "Revendeur"),

                    ('save upto 20 % on each product', 'ahorre hasta un 20 % en cada producto',
                     "économisez jusqu'à 20 % sur chaque produit", 0.20, "Discontinued Product", "Descatalogado",
                     "Ce produit n'est plus commercialisé", "Reseller", "Distribuidor", "Revendeur")]

geography_data= [(12, 'NorthEastInd', 'Arunachal Pradesh', 'AR', 'IND', ['Itanagar', 'Tawang', 'Ziro', 'Namasi', 'Along']),
            (12, 'NorthEastInd', 'Assam', 'AS', 'IND', ['Guwathi', 'Jorhat', 'Silchar', 'Tezpur', 'Nagoan']),
            (12, 'NorthEastInd', 'Meghalaya', 'ML', 'IND',
             ['Shillong', 'Cherrapunjee', 'Tura', 'Jowai', 'Nongstoin', 'Baghmara']),
            (12, 'NorthEastInd', 'Manipur', 'MN', 'IND',
             ['Imphal', 'Thoubal', 'Bishnupur', 'Churachandpur', 'Senapati', 'Ukhrul', 'Tamenglong']),
            (12, 'NorthEastInd', 'Mizoram', 'MZ', 'IND',
             ['Aizawl', 'Lunglei', 'Champhai', 'Saiha', 'Serchhip', 'Kolasib', 'Mamit']),
            (12, 'NorthEastInd', 'Nagaland', 'NL', 'IND',
             ['Kohima', 'Dimapur', 'Mokokchung', 'Tuensang', 'Wokha', 'Zunheboto', 'Phek']),
            (12, 'NorthEastInd', 'Sikkim', 'SK', 'IND', ['Gangtok', 'Namchi', 'Rangpo', 'Jorethang', 'Mangan']),
            (12, 'NorthEastInd', 'Tripura', 'TR', 'IND',
             ['Agartala', 'Udaipur', 'Dharmanagar', 'Kailasahar', 'Belonia']),
            (13, 'NorthWestInd', 'Jammu and Kashmir', 'JK', 'IND',
             ['Srinagar', 'Jammu', 'Anantnag', 'Baramulla', 'Kathua', 'Pulwama', 'Udhampur', 'Sopore']),
            (13, 'NorthWestInd', 'Himachal Pradesh', 'HP', 'IND',
             ['Shimla', 'Manali', 'Dharamshala', 'Solan', 'Kullu', 'Hamirpur', 'Mandi']),
            (13, 'NorthWestInd', 'Punjab', 'PB', 'IND', ['Chandigarh', 'Ludhiana', 'Amritsar', 'Jalandhar', 'Patiala']),
            (13, 'NorthWestInd', 'Haryana', 'HR', 'IND',
             ['Gurgaon', 'Faridabad', 'Chandigarh', 'Rohtak', 'Hisar', 'Panipat', 'Ambala']),
            (13, 'NorthWestInd', 'Rajasthan', 'RJ', 'IND',
             ['Jaipur', 'Jodhpur', 'Udaipur', 'Ajmer', 'Kota', 'Bikaner', 'Jaisalmer']),
            (13, 'NorthWestInd', 'Uttarakhand', 'UT', 'IND',
             ['Dehradun', 'Haridwar', 'Rishikesh', 'Nainital', 'Mussoorie', 'Almora', 'Haldwani']),
            (13, 'NorthWestInd', 'Uttar Pradesh', 'UP', 'IND',
             ['Lucknow', 'Kanpur', 'Agra', 'Varanasi', 'Allahabad', 'Ghaziabad', 'Noida']),
            (13, 'NorthWestInd', 'Delhi', 'DL', 'IND', ['New Delhi', 'Old Delhi', 'Noida', 'Gurgaon', 'Faridabad']),
            (14, 'SouthEastInd', 'Andhra Pradesh', 'AP', 'IND',
             ['Visakhapatnam', 'Vijayawada', 'Guntur', 'Tirupati', 'Kakinada', 'Rajahmundry', 'Nellore']),
            (14, 'SouthEastInd', 'Karnataka', 'KA', 'IND',
             ['Bengaluru', 'Mysuru', 'Hubli', 'Mangaluru', 'Belagavi', 'Shivamogga', 'Davangere']),
            (14, 'SouthEastInd', 'Kerala', 'KL', 'IND',
             ['Thiruvananthapuram', 'Kochi', 'Kozhikode', 'Thrissur', 'Kollam', 'Alappuzha', 'Palakkad']),
            (14, 'SouthEastInd', 'Tamil Nadu', 'TN', 'IND',
             ['Chennai', 'Coimbatore', 'Madurai', 'Tiruchirappalli', 'Salem', 'Tirunelveli', 'Vellore']),
            (14, 'SouthEastInd', 'Telangana', 'TS', 'IND',
             ['Hyderabad', 'Warangal', 'Karimnagar', 'Khammam', 'Nizamabad', 'Secunderabad', 'Nalgonda']),
            (15, 'SouthWestInd', 'Maharashtra', 'MH', 'IND', ['Mumbai', 'Pune', 'Nagpur', 'Nashik', 'Solapur']),
            (15, 'SouthWestInd', 'Goa', 'GA', 'IND', ['Panaji', 'Vasco da Gama', 'Margao', 'Mapusa', 'Ponda'])]