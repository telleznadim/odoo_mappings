import xmlrpc.client
import pandas as pd
from dotenv import dotenv_values
import ast
import re
import math

config = dotenv_values(".env") 

username = config["username"]
password = config["password"]
bc_ids = pd.read_csv("files/relations/bc_ids.csv")
brands = pd.read_csv("files/relations/brands.csv")
product_category = pd.read_csv("files/relations/product_category.csv")
inv_posting_group = pd.read_csv("files/relations/inv_posting_group.csv")
gen_posting_group = pd.read_csv("files/relations/gen_posting_group.csv")
supplierinfo = pd.read_csv("files/relations/supplierinfo.csv")


def getColumns(df):
    for col in df.columns:
        print(col)

def getSupplierInfoTable():
    url = "https://laundry-south.odoo.com"
    db = "captivea-usa-laundrysouth2021-main-1880046"

    common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
    uid = common.authenticate(db, username, password, {})

    models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))
    records_count = models.execute_kw(db, uid, password, 'product.supplierinfo', 'search_count', [[]])
    print(records_count)
    print(records_count/5000)
    limit = 5000
    df = pd.DataFrame()
    
    for x in range(0,math.ceil(records_count/limit)):
        offset = x * limit
        print("numbers ",x)
        print("offset ",offset)
        ids = models.execute_kw(db, uid, password, 'product.supplierinfo', 'search', [[]], {'offset': offset, 'limit': limit})
        print("Lenght ",len(ids))
        record = models.execute_kw(db, uid, password, 'product.supplierinfo', 'read', [ids],{'fields': ["id","name"]})
        df1 = pd.DataFrame.from_dict(record)
        df = pd.concat([df,df1])
    df.rename(columns={"id":"old_value","name":"new_value"}, inplace=True)
    df.to_csv("files/relations/supplierinfo.csv",index=False)
    # id	name	company_id	display_name

def captureDataFromApi():
    url = "https://laundry-south.odoo.com"
    db = "captivea-usa-laundrysouth2021-main-1880046"

    common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
    uid = common.authenticate(db, username, password, {})

    models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))
    # ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['customer_rank', '>', 0]]])
    # ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['supplier_rank', '=', 0]]])
    
    records_count = models.execute_kw(db, uid, password, 'product.product', 'search_count', [[]])
    # '|',['qty_available', '>', 0],
    print(records_count)
    print(records_count/5000)
    limit = 5000
    df = pd.DataFrame()
    
    for x in range(0,math.ceil(records_count/limit)):
        offset = x * limit
        print("numbers ",x)
        print("offset ",offset)

       
        ids = models.execute_kw(db, uid, password, 'product.product', 'search', [[]], {'offset': offset, 'limit': limit})
        
        print("Lenght ",len(ids))
        record = models.execute_kw(db, uid, password, 'product.product', 'read', [ids],{'fields': ["id","default_code","code","partner_ref","product_tmpl_id","standard_price","cost_method","display_name","__last_update","name","description","description_sale","categ_id","list_price","uom_id","uom_name","uom_po_id","product_variant_id","x_studio_brand","x_studio_related_field_JH8QU","seller_ids","qty_available", "sales_count"]})
        # record = models.execute_kw(db, uid, password, 'product.product', 'read', [ids])

        df1 = pd.DataFrame.from_dict(record)
        df = pd.concat([df,df1])

    df.to_csv("files/csvs/from_api/items4.csv",index=False)

def getUniqueFromColumn(df):
    for item in df.unique():
        print(item)
    print(len(df.unique()))

# def getStateAbbre(long_state):
#     # transform data received
#     long_state = ast.literal_eval(long_state)

#     # Calculate short State
#     short_state = ""
#     if (long_state):
#         long_state = long_state[1]
#         if('(US)' in long_state):
#             long_state = long_state[:-5]
#             short_state = states.loc[states["long_state"] == long_state]['short_state'].values[0]
#         # elif('(IN)' in long_state):
#         #     print(False)

#     # else:
#     #     print(2)
#     #     print(long_state)
    
#     return(short_state)

def setBlocked(active):
    blocked = ""
    if(not active):
        blocked = "All"

    return blocked

def getCountryAbbre(long_country):
    # transform data received
    long_country = ast.literal_eval(long_country)

    # Calculate short State
    short_country = ""
    
    if (long_country):
        long_country = long_country[1]
        if (long_country == "United States"):
            short_country = "US"
        # if('(US)' in long_country):
        #     long_country = long_country[:-5]
        #     short_state = states.loc[states["long_state"] == long_state]['short_state'].values[0]
        # elif('(IN)' in long_state):
        #     print(False)

    # else:
    #     print(2)
    #     print(long_state)
    
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

def getMappingList(old_value,relation_csv,default_value):
    old_value = ast.literal_eval(old_value)

    new_value = default_value
    if (old_value != False):
        find_new = relation_csv.loc[relation_csv["old_value"] == old_value[0]]['new_value']
        # print(find_new)
        if (not find_new.empty):
            new_value = relation_csv.loc[relation_csv["old_value"] == old_value[0]]['new_value'].values[0]
    
    return(new_value)

def getInBc(old_value,relation_csv,default_value):
    old_value = ast.literal_eval(old_value)

    new_value = default_value
    if (old_value != False):
        find_new = relation_csv.loc[relation_csv["old_value"] == old_value[0]]['in_bc']
        # print(find_new)
        if (not find_new.empty):
            new_value = relation_csv.loc[relation_csv["old_value"] == old_value[0]]['in_bc'].values[0]
    
    return(new_value)

def getProductCategory(old_value):
    old_value = ast.literal_eval(old_value)

    new_value = old_value[0]
    if (new_value != "False"):
        find_new = product_category.loc[product_category["old_value"] == new_value]['new_value']
        # print(find_new)
        if (not find_new.empty):
            new_value = product_category.loc[product_category["old_value"] == new_value]['new_value'].values[0]
    else:
        new_value = ""
    
    return(new_value)

def getVendorNo(old_value):
    old_value = ast.literal_eval(old_value)

    new_value = ""
    if (len(old_value) > 0):
        find_new = supplierinfo.loc[supplierinfo["old_value"] == old_value[0]]['new_value']
        new_value = ast.literal_eval(find_new.values[0])[0]
        
    return(new_value)

def getUnitOfMeasure(old_value):
    
    new_value = ""
    if (old_value == "ft"):
        new_value = "FT"
    elif (old_value == "Hours"):
        new_value = "HOURS"
    elif (old_value == "Units"):
        new_value = "EA"

    # print(new_value)
    return(new_value)



def getPurchaseUnitOfMeasure(old_value):
    old_value = ast.literal_eval(old_value)

    new_value = ""
    if (old_value[0] == 16):
        new_value = "FT"
    elif (old_value[0] == 6):
        new_value = "HOURS"
    elif (old_value[0] == 1):
        new_value = "EA"

    # print(new_value)
    return(new_value)

def getBrand(old_value):
    new_value = old_value
    if (new_value != "False"):
        find_new = brands.loc[brands["old_value"] == new_value]['new_value']
        if (not find_new.empty):
            new_value = brands.loc[brands["old_value"] == new_value]['new_value'].values[0]
    else:
        new_value = "ZZN"
    return(new_value)

def getInventoryPostingGroup(old_value):
    old_value = ast.literal_eval(old_value)

    new_value = old_value[0]
    if (new_value != "False"):
        find_new = inv_posting_group.loc[inv_posting_group["old_value"] == new_value]['new_value']
        # print(find_new)
        if (not find_new.empty):
            new_value = inv_posting_group.loc[inv_posting_group["old_value"] == new_value]['new_value'].values[0]
    else:
        new_value = ""
    
    return(new_value)

def getGenProdPostingGroup(old_value):
    old_value = ast.literal_eval(old_value)

    new_value = old_value[0]
    if (new_value != "False"):
        find_new = gen_posting_group.loc[gen_posting_group["old_value"] == new_value]['new_value']
        # print(find_new)
        if (not find_new.empty):
            new_value = gen_posting_group.loc[gen_posting_group["old_value"] == new_value]['new_value'].values[0]
    else:
        new_value = ""
    
    return(new_value)

def readItemsTransform():
    df = pd.read_csv("files/csvs/from_api/items4.csv")
    print(df)
    df = df[(df['qty_available'] > 0) | (df["sales_count"] != 0)]
    print(df)
    # print(getColumns(df))
    # print(getUniqueFromColumn(df["product_tmpl_id"]))
    # print(getUniqueFromColumn(df["uom_name"]))
    # print(getUniqueFromColumn(df["cost_method"]))
    
    # print(getUniqueFromColumn(df["uom_id"]))
    # print(getUniqueFromColumn(df["uom_po_id"]))
    # print(getUniqueFromColumn(df["x_studio_brand"]))
    # print(getUniqueFromColumn(df["seller_ids"]))

    
    

    # Get rid of False values
    df["Manufacturer Item No."] = df["default_code"].str.replace("False","")
    df['Description'] = df['name'].str.replace("False","")
    df['Description 2'] = df['description'].str.replace("False","")
    df["Legacy ID No."] = df["default_code"].str.replace("False","")
    # df['standard_price'] = df['standard_price'].str.replace("False","")

    # # Transform DATA
    print("Getting No.")
    df["No."] = df["product_tmpl_id"].apply(getMappingList,args=(bc_ids,"MISSING",))
    df["in_bc"] = df["product_tmpl_id"].apply(getInBc,args=(bc_ids,False,))
    
    print("Mapping Product Category")
    df["Item Category Code"] = df["categ_id"].apply(getProductCategory)
    print("Get Vendor No.")
    df["seller_ids"] = df["seller_ids"].apply(getVendorNo)
    print("Getting Unit Of Measure")
    df["uom_name"] = df["uom_name"].apply(getUnitOfMeasure)
    print("Getting Purchase Unit Of Measure")
    df["uom_po_id"] = df["uom_po_id"].apply(getPurchaseUnitOfMeasure)
    print("Getting Brands")
    df["x_studio_brand"] = df["x_studio_brand"].apply(getBrand)
    print("Getting Inventory Posting Group")
    df["Inventory Posting Group"] = df["categ_id"].apply(getInventoryPostingGroup)
    print("Getting Gen. Prod. Posting Group")
    df["Gen. Prod. Posting Group"] = df["categ_id"].apply(getGenProdPostingGroup)
    

    
    # df["zip"] = df["zip"].apply(cleanPhoneNo)
    # df["mobile"] = df["mobile"].apply(cleanPhoneNo)
    # df["phone_sanitized"] = df["phone_sanitized"].apply(cleanPhoneNo)
   
    # df["active"] = df["active"].apply(setBlocked)
    # df["country_id"] = df["country_id"].apply(getCountryAbbre)
    # df['company_type'] = df['company_type'].str.capitalize()
    # df["property_account_position_id"] = df["property_account_position_id"].apply(getTax)
    # df["property_payment_term_id"] = df["property_payment_term_id"].apply(getPaymentTerm)

    # # getUniqueFromColumn(df["x_studio_customer_type"])
    # df["x_studio_customer_type"] = df["x_studio_customer_type"].apply(getCustomerVertical)
    # df["user_id"] = df["user_id"].apply(getUserId)
    # # print(df["country_id"])

    
    
    
   
    df.rename(columns={"seller_ids":"Vendor No.", "uom_name":"Base Unit of Measure","list_price":"Unit Price","standard_price":"Last Direct Cost","__last_update":"Last Date Modified","uom_po_id":"Purch. Unit of Measure","x_studio_brand":"Brand","id":"Manufacturer Code"}, inplace=True)
    # "display_name": "Name", "website":"Home Page","credit_limit":"Credit Limit ($)","active":"Blocked", "street":"Address","street2":"Address 2","zip":"ZIP Code","city":"City","state_id":"State","country_id":"Country/Region Code", "mobile":"Mobile Phone No.","company_type":"Partner Type","email_normalized":"Email","commercial_company_name":"Name 2","phone_sanitized":"Phone No.","property_account_position_id":"SureTax© Exemption Code","x_studio_customer_type":"Customer Vertical","property_payment_term_id":"Payment Terms Code","user_id":"Salesperson Code","create_date":"Customer Since"

    # Mappings
    df["Costing Method"] = df['cost_method'].str.replace("standard","AVERAGE")
    df['Item Tracking Code'] = df['cost_method'].str.replace("standard","AVG")

    # # Static Columns
    df["Sales Unit of Measure"] = df["Base Unit of Measure"]
    df["Unit Cost"] = df["Last Direct Cost"]
    df["Unit List Price"] = df["Unit Price"]
    df["Type"] = "Inventory"
    df["Tax Group Code"] = "SURETAX"
    df["Country/Region of Origin Code"] = "US"
    df["Purchasing Code"] = "DROPSHIP"
    df["Country/Region Purchased Code"] = "US"
    df["Vendor Item No."] = ""
    df["Global Dimension 2 Code"] = ""
    df["Division"] = ""
    df["Blocked"] = ""

    
    
    
    
    # df["Location Code"] = "WH1"
    # df["Tax Area Code"] = "STX_"
    # df["Contact"] = ""
    # df["Territory Code"] = ""
    # df["Shipment Method Code"] = "GROUND"
    # df["Customer Disc. Group"] = ""
    # df["Fax No."] = ""
    # df["EORI Number"] = ""
    # df["Tax Liable"] = "True"
    # df["Purchase Order Required"] = ""
    # df["Legacy Note ID"] = ""
    # df["Legacy Addr 1"] = ""
    # df["Legacy Addr 2"] = ""
    
    
    df_columns = ["No.","Item Category Code","Vendor No.","Description","Base Unit of Measure","Unit Price","Last Direct Cost","Item Tracking Code","Description 2","Last Date Modified","Sales Unit of Measure","Purch. Unit of Measure","Brand","Unit Cost","Unit List Price","Legacy ID No.","Type","Tax Group Code","Country/Region of Origin Code","Purchasing Code","Country/Region Purchased Code","Vendor Item No.","Inventory Posting Group","Gen. Prod. Posting Group","Costing Method","Global Dimension 2 Code","Division","Blocked","Manufacturer Code","Manufacturer Item No.","product_tmpl_id","in_bc"]
    # ,"Customer Since","Name","Home Page","Credit Limit ($)","Blocked","Address","Address 2","ZIP Code","City","State","Country/Region Code","Mobile Phone No.","Partner Type","Name 2","Email","Phone No.","SureTax© Exemption Code","Customer Vertical","Payment Terms Code","Salesperson Code","Customer Posting Group","Gen. Bus. Posting Group","Bill-to Customer No.","Location Code","Tax Area Code","Contact","Territory Code","Shipment Method Code","Customer Disc. Group","Fax No.","EORI Number","Tax Liable","Purchase Order Required","Legacy Note ID","Legacy Addr 1","Legacy Addr 2"
    df = df.reindex(columns=df_columns)
    # print(getColumns(df))
    print(df)
    df2 = df[df['in_bc']]
    df2 = df2.filter(["No.","Manufacturer Code","Manufacturer Item No."], axis=1)
    df_columns = ["No.","Item Category Code","Vendor No.","Description","Base Unit of Measure","Unit Price","Last Direct Cost","Item Tracking Code","Description 2","Last Date Modified","Sales Unit of Measure","Purch. Unit of Measure","Brand","Unit Cost","Unit List Price","Legacy ID No.","Type","Tax Group Code","Country/Region of Origin Code","Purchasing Code","Country/Region Purchased Code","Vendor Item No.","Inventory Posting Group","Gen. Prod. Posting Group","Costing Method","Global Dimension 2 Code","Division","Blocked","Manufacturer Code","Manufacturer Item No.","product_tmpl_id","in_bc"]
    df2 = df2.reindex(columns=df_columns,fill_value="")
    print(df2)

    df_final = pd.concat([df[~df['in_bc']],df2])
    print(df_final)
    
    # print(getColumns(df))
    # df.to_excel("files/to_bc_outputs/customers2_output.xlsx",index=False)
    # df.to_excel("files/to_bc_outputs/contacts_output.xlsx",index=False)
    df_final.to_excel("files/to_bc_outputs/items_output_07112022_3.xlsx",index=False)

# getSupplierInfoTable()
# captureDataFromApi()
readItemsTransform()