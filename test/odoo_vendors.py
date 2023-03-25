import xmlrpc.client
import pandas as pd
from dotenv import dotenv_values
import ast
import re

config = dotenv_values(".env") 

username = config["username"]
password = config["password"]
states = pd.read_csv("files/relations/states.csv")
cstvrt = pd.read_csv("files/relations/CSTVRT.csv")
salesperson = pd.read_csv("files/relations/salesperson.csv")


def getColumns(df):
    print(df.columns)
    for col in df.columns:
        print(col)

def captureDataFromApi():
    url = "https://laundry-south.odoo.com"
    db = "captivea-usa-laundrysouth2021-main-1880046"

    common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
    uid = common.authenticate(db, username, password, {})

    models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))
    # Query
    ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['supplier_rank', '>', 0]]])
    record = models.execute_kw(db, uid, password, 'res.partner', 'read', [ids], {'fields': ['id', 'display_name','active','street','street2','zip','city','state_id','country_id','email_normalized','phone_sanitized','commercial_company_name','property_payment_term_id',"create_date","__last_update"]})
   
    df = pd.DataFrame.from_dict(record)
    print(df)
    df.to_csv("files/csvs/from_api/vendors.csv",index=False)

def getUniqueFromColumn(df):
     for item in df.unique():
        print(item)


def getStateAbbre(long_state):
    # transform data received
    long_state = ast.literal_eval(long_state)

    # Calculate short State
    short_state = ""
    if (long_state):
        long_state = long_state[1]
        if('(US)' in long_state):
            long_state = long_state[:-5]
            short_state = states.loc[states["long_state"] == long_state]['short_state'].values[0]
    
    return(short_state)

def setBlocked(active):
    blocked = ""
    if(not active):
        blocked = "All"

    return blocked

def getCountryAbbre(long_country):
    # transform data received
    long_country = ast.literal_eval(long_country)

    # Calculate short country
    short_country = ""
    
    if (long_country):
        long_country = long_country[1]
        if (long_country == "United States"):
            short_country = "US"

    
    return(short_country)

def getTax(tax_position):
    tax_position = ast.literal_eval(tax_position)
    bc_tax = ""
    if (tax_position):
        tax_position = tax_position[0]
        if(tax_position == 192):
            bc_tax = 99
    return(bc_tax)

def getPaymentTerm(payment_term):
    payment_term = ast.literal_eval(payment_term)
    bc_payment_term_code = "COD"
    if (payment_term):
        payment_term = payment_term[0]
        
        if(payment_term == 9): # [9, 'Due on delivery']
            bc_payment_term_code = "COD"
        elif(payment_term == 12): # [12, 'Credit Card Before Dispatch']
            bc_payment_term_code = "CBS"
        elif(payment_term == 4): # [4, '30 Days']
            bc_payment_term_code = "NET30"
        elif(payment_term == 2): # [2, '15 Days']
            bc_payment_term_code = "NET15"
        elif(payment_term == 8): # [8, '50% Now, Balance on Delivery']
            bc_payment_term_code = "ZZ-REVIEW"
    return(bc_payment_term_code)

def cleanPhoneNo(phone_no):
    return(re.sub('[^0-9 *+*-]','', phone_no))



def readContactsTransform():
    df = pd.read_csv("files/csvs/from_api/vendors.csv")
    print(df)

    # Get rid of False values
    df['street'] = df['street'].str.replace("False","")
    df['street2'] = df['street2'].str.replace("False","")
    df['city'] = df['city'].str.replace("False","")
    df['email_normalized'] = df['email_normalized'].str.replace("False","")
    df['commercial_company_name'] = df['commercial_company_name'].str.replace("False","")
  
    df["zip"] = df["zip"].apply(cleanPhoneNo)
    df["phone_sanitized"] = df["phone_sanitized"].apply(cleanPhoneNo)
    df["state_id"] = df["state_id"].apply(getStateAbbre)
    df["active"] = df["active"].apply(setBlocked)
    df["country_id"] = df["country_id"].apply(getCountryAbbre)
    df["property_payment_term_id"] = df["property_payment_term_id"].apply(getPaymentTerm)


    
    
    
   
    df.rename(columns={"id": "No.", "display_name": "Name","active":"Blocked", "street":"Address","street2":"Address 2","zip":"ZIP Code","city":"City","state_id":"State","country_id":"Country/Region Code","email_normalized":"Email","commercial_company_name":"Name 2","phone_sanitized":"Phone No.","property_payment_term_id":"Payment Terms Code","create_date":"Vendor Since","__last_update":"Last Date Modified"}, inplace=True)
    
    # Static Columns
    df["Gen. Bus. Posting Group"] = "STD"
    
    df["Contact"] = ""
    df["Shipment Method Code"] = "GROUND"
    df["Fax No."] = ""
  
    df["Legacy Addr 1"] = ""
    df["Legacy Addr 2"] = ""
    df["Federal ID No."] = ""
    df["IRS 1099 Code"] = ""
    df["Our Account No."] = ""
    df["Payment Method Code"] = ""
    df["Search Name"] = ""
    df["Shipping Agent Code"] = ""
    df["Vendor Posting Group"] = "STD"
    df["Tax Area Code"] = "STX_"
    df["Tax Liable"] = "True"
    df["Brand"] = ""
    
    df_columns = ["No.","Name","Address","Address 2","City","State","ZIP Code","Payment Terms Code","Contact","Phone No.","IRS 1099 Code","Federal ID No.","Blocked","Fax No.","Email","Country/Region Code","Vendor Since","Legacy Addr 1","Legacy Addr 2","Search Name","Name 2","Vendor Posting Group","Gen. Bus. Posting Group","Our Account No.","Shipment Method Code","Shipping Agent Code","Payment Method Code","Last Date Modified","Tax Area Code","Tax Liable","Brand"]



    
    
    
    print(getColumns(df))
    df = df.reindex(columns=df_columns)
    print(getColumns(df))

    print(df)
    
    # print(getColumns(df))
    df.to_excel("files/to_bc_outputs/vendors_output.xlsx",index=False)




# captureDataFromApi()
readContactsTransform()