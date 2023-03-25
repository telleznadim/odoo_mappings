import xmlrpc.client
import pandas as pd
from dotenv import dotenv_values
import ast
import math
from datetime import datetime

config = dotenv_values(".env") 


date_time = datetime.now()
username = config["username"]
password = config["password"]
bc_ids = pd.read_excel("files/relations/items/bc_ids.xlsx")
location_code = pd.read_csv("files/relations/sale_order/location_code.csv")
document_type = pd.read_csv("files/relations/sale_order/document_type.csv")
sales_person = pd.read_csv("files/relations/sale_order/sales_person.csv")
payment_term = pd.read_csv("files/relations/sale_order/payment_term.csv")
customers = pd.read_excel("files/to_bc_outputs/customers_output_True.xlsx")
item_type = pd.read_csv("files/relations/sale_order/item_type.csv")


def getColumns(df):
    for col in df.columns:
        print(col)


def captureSaleOrderHeaderHeadersDataFromApi():
    url = "https://laundry-south.odoo.com"
    db = "captivea-usa-laundrysouth2021-main-1880046"

    common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
    uid = common.authenticate(db, username, password, {})

    models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))
    # ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['customer_rank', '>', 0]]])
    # ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['supplier_rank', '=', 0]]])
    
    records_count = models.execute_kw(db, uid, password, 'sale.order', 'search_count', [[]])
    print(records_count)
    print(records_count/5000)
    limit = 5000
    df = pd.DataFrame()
    
    for x in range(0,math.ceil(records_count/limit)):
        offset = x * limit
        print("numbers ",x)
        print("offset ",offset)
        ids = models.execute_kw(db, uid, password, 'sale.order', 'search', [[]], {'offset': offset, 'limit': limit})
        print("Lenght ",len(ids))
        record = models.execute_kw(db, uid, password, 'sale.order', 'read', [ids],{'fields': ["name","warehouse_id","date_order","expected_date","partner_id","x_studio_customer_po","user_id","x_studio_delivery_status","payment_term_id","activity_summary","invoice_ids","partner_id","partner_shipping_id","type_name","origin"]})
        # record = models.execute_kw(db, uid, password, 'sale.order', 'read', [ids])
        df1 = pd.DataFrame.from_dict(record)
        df = pd.concat([df,df1])

    df.to_csv("files/csvs/from_api/sales_orders.csv",index=False)
    df.to_csv("files/csvs/from_api/history/sales_orders_" + date_time.strftime("%m%d%y_%H%M%S") + ".csv.gz",index=False, compression='gzip')


def captureSaleOrderLinesDataFromApi():
    url = "https://laundry-south.odoo.com"
    db = "captivea-usa-laundrysouth2021-main-1880046"

    common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
    uid = common.authenticate(db, username, password, {})

    models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))
    # ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['customer_rank', '>', 0]]])
    # ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['supplier_rank', '=', 0]]])
    
    records_count = models.execute_kw(db, uid, password, 'sale.order.line', 'search_count', [[]])
    print(records_count)
    print(records_count/5000)
    limit = 5000
    df = pd.DataFrame()
    
    for x in range(0,math.ceil(records_count/limit)):
        offset = x * limit
        print("numbers ",x)
        print("offset ",offset)
        ids = models.execute_kw(db, uid, password, 'sale.order.line', 'search', [[]], {'offset': offset, 'limit': limit})
        print("Lenght ",len(ids))
        record = models.execute_kw(db, uid, password, 'sale.order.line', 'read', [ids])
        df1 = pd.DataFrame.from_dict(record)
        df = pd.concat([df,df1])

    df.to_csv("files/csvs/from_api/sales_orders_lines.csv",index=False)
    df.to_csv("files/csvs/from_api/history/sales_orders_lines_" + date_time.strftime("%m%d%y_%H%M%S") + ".csv.gz",index=False, compression='gzip')
    

def getUniqueFromColumn(df):
    for item in df.unique():
        print(item)
    



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


def getMappingList(old_value,relation_csv,default_value):
    old_value = ast.literal_eval(old_value)

    new_value = default_value
    if (old_value != False):
        find_new = relation_csv.loc[relation_csv["old_value"] == old_value[0]]['new_value']
        # print(find_new)
        if (not find_new.empty):
            new_value = relation_csv.loc[relation_csv["old_value"] == old_value[0]]['new_value'].values[0]
    
    return(new_value)

def getMapping(old_value,relation_csv, default_value):
    new_value = old_value
    if (new_value != "False"):
        find_new = relation_csv.loc[relation_csv["old_value"] == new_value]['new_value']
        # print(find_new)
        if (not find_new.empty):
            new_value = relation_csv.loc[relation_csv["old_value"] == new_value]['new_value'].values[0]
    else:
        new_value = default_value
    return(new_value)
    # print(relation_csv)

def getFirstPositionList(old_value):
    old_value = ast.literal_eval(old_value)
    if(old_value):
        return(old_value[0])
    else:
        return("")

def getSecondPositionList(old_value):
    old_value = ast.literal_eval(old_value)
    if(old_value):
        return(old_value[1])
    else:
        return("")


def getCustomerInfo(customer_id, value_name):
    new_value = ""
    if (customer_id != "False"):
        find_new = customers.loc[customers["No."] == customer_id][value_name]
        if (not find_new.empty):
            if (not find_new.isnull().values.any()):
                new_value = customers.loc[customers["No."] == customer_id][value_name].values[0]
        
    # print(new_value)
    return(new_value)

def getBillInfo(customer_id):
    bill_info = {"Address": "", "Address 2":"", "City":"", "State":"", "ZIP Code":""}

    if (customer_id != "False"):
        find_new = customers.loc[customers["No."] == customer_id]
        for value_name in bill_info:
            if (not find_new[value_name].empty):
                if (not find_new[value_name].isnull().values.any()):
                    bill_info[value_name] = find_new[value_name].values[0]
            else:
                bill_info[value_name] = "not_found"
    return(bill_info)

def getShipInfo(customer_id):
    bill_info = {"Name":"","Address": "", "Address 2":"", "City":"", "State":"", "ZIP Code":""}
    
    if (customer_id != "False"):
        find_new = customers.loc[customers["No."] == customer_id]
        for value_name in bill_info:
            if (not find_new[value_name].empty):
                if (not find_new[value_name].isnull().values.any()):
                    bill_info[value_name] = find_new[value_name].values[0]
            else:
                bill_info[value_name] = "not_found"
    return(bill_info)

def getSaleOrderInfo(sale_order_id, sale_orders_list):
    bill_info = {"Document Type":""}
    # print(sale_order_id)

    if (sale_order_id != "False"):
        find_new = sale_orders_list.loc[sale_orders_list["No."] == sale_order_id]
        # print(find_new)
        for value_name in bill_info:
            if (not find_new[value_name].empty):
                if (not find_new[value_name].isnull().values.any()):
                    bill_info[value_name] = find_new[value_name].values[0]

    # print(bill_info)
    return(bill_info["Document Type"])

# getBillInfo(2835)

def getBillAddress(df):
    # print(df)
    customer_id = ast.literal_eval(df["partner_id"])[0]
    customer_shipp_id = ast.literal_eval(df["partner_shipping_id"])[0]
    
    bill_info = getBillInfo(customer_id)
    df["Address"] = bill_info["Address"]
    df["Address 2"] = bill_info["Address 2"] 
    df["City"] = bill_info["City"] 
    df["State"] = bill_info["State"]
    df["ZIP Code"] = bill_info["ZIP Code"]

    ship_info = getShipInfo(customer_shipp_id)
    df["Ship-to Name"] = ship_info["Name"]
    df["Ship-to Address"] = ship_info["Address"]
    df["Ship-to Address 2"] = ship_info["Address 2"] 
    df["Ship-to City"] = ship_info["City"] 
    df["Ship-to State"] = ship_info["State"]
    df["Ship-to ZIP Code"] = ship_info["ZIP Code"]

    # df["Address"] = getCustomerInfo(customer_id, "Address")
    # df["Address 2"] = getCustomerInfo(customer_id, "Address 2")
    # df["City"] = getCustomerInfo(customer_id, "City")
    # df["State"] = getCustomerInfo(customer_id, "State")
    # df["Zip Code"] = getCustomerInfo(customer_id, "ZIP Code")

    # df["Ship-to Name"] = getCustomerInfo(customer_shipp_id, "Name")
    # df["Ship-to Address"] = getCustomerInfo(customer_shipp_id, "Address")
    # df["Ship-to Address 2"] = getCustomerInfo(customer_shipp_id, "Address 2")
    # df["Ship-to City"] = getCustomerInfo(customer_shipp_id, "City")
    # df["Ship-to State"] = getCustomerInfo(customer_shipp_id, "State")
    # df["Ship-to Zip Code"] = getCustomerInfo(customer_shipp_id, "ZIP Code")
    
    # find_new = customers.loc[customers["No."] == customer_id]["Address"]
    # if (customer_id != "False"):
    #     print(customer_id)
    #     find_new = customers.loc[customers["No."] == customer_id]['Address']
    #     # print(find_new)
    #     if (not find_new.empty):
    #         df["Address"] = customers.loc[customers["No."] == customer_id]['Address'].values[0]
    # else:
    #     df["Address"] = ""
    # print(new_value)
    # # print(find_new)
    # # print(customer_id)
    # return(new_value)
    return(df)



def readSaleOrderHeaderHeadersTransform():
    df = pd.read_csv("files/csvs/from_api/sales_orders.csv")
    print(df)
    print(customers)
    print(getColumns(df))
    
    print("......Unique Values......")
    # print(getUniqueFromColumn(df["warehouse_id"]))
    # print(getUniqueFromColumn(df["type_name"]))
    # print(getUniqueFromColumn(df["x_studio_customer_po"]))
    
    # print(getUniqueFromColumn(df["user_id"]))
    # print(getUniqueFromColumn(df["x_studio_delivery_status"]))
    # print(getUniqueFromColumn(df["x_studio_related_field_d5eNv"]))
    # print(getUniqueFromColumn(df["seller_ids"]))
    print("......Unique Values......")
    
    

    # Get rid of False values
    print("Deleting False values from some columns")
    df['Shipment Date'] = df['expected_date'].str.replace("False","")
    del df['expected_date']
    df['External Document No.'] = df['x_studio_customer_po'].str.replace("False","")
    del df["x_studio_customer_po"]
    # df["Shipment Method Code"] = df["x_studio_delivery_status"].str.replace("False","")
    del df["x_studio_delivery_status"]
    df["Work Description"] = df["activity_summary"].str.replace("False","")
    del df["activity_summary"]
    df["Your Reference"] = df["origin"].str.replace("False","")
    del df["origin"]


    
    # # Transform DATA
    print("Location Code")
    df["Location Code"] = df["warehouse_id"].apply(getMappingList,args=(location_code,"",))
    del df['warehouse_id']
    print("Document Type")
    df["Document Type"] = df["type_name"].apply(getMapping,args=(document_type,"",))
    del df['type_name']
    print("Sell-to Customer No.")
    df["Sell-to Customer No."] = df["partner_id"].apply(getFirstPositionList)
    print("Getting Sales Person Code")
    df["Salesperson Code"] = df["user_id"].apply(getMappingList,args=(sales_person,"",))
    del df['user_id']
    print("Getting Payment Terms Code")
    df["Payment Terms Code"] = df["payment_term_id"].apply(getMappingList,args=(payment_term,"COD",))
    del df['payment_term_id']
    print("Getting Bill-to and Ship-to Addresses")
    df = df.apply(getBillAddress, axis=1)
   
    df.rename(columns={"name": "No.","date_order":"Order Date"}, inplace=True)
    

    # # # Static Columns
    df["Document Date"] = df["Order Date"]
    df["Invoice"] = ""
    df["Shipment Method Code"] = ""
    
    
    df_columns = ["No.","Location Code","Order Date","Shipment Date","Document Type","Sell-to Customer No.","External Document No.","Salesperson Code","Shipment Method Code","Payment Terms Code","Work Description","Invoice","Address","Address 2","City","State","Zip Code","Ship-to Name","Ship-to Address","Ship-to Address 2","Ship-to City","Ship-to State","Ship-to ZIP Code","Your Reference","Document Date"]
    df = df.reindex(columns=df_columns)
    # print(getColumns(df))
    print(df)
    
    # print(getColumns(df))
    # df.to_excel("files/to_bc_outputs/customers2_output.xlsx",index=False)
    # df.to_excel("files/to_bc_outputs/contacts_output.xlsx",index=False)
    df.to_csv("files/to_bc_outputs/history/sales_orders_headers_output_" + date_time.strftime("%m%d%y_%H%M%S") + ".csv",index=False, compression='gzip')
    df.to_excel("files/to_bc_outputs/sales_orders_headers_output.xlsx",index=False)


def readSaleOrderLinesTransform():
    df = pd.read_csv("files/csvs/from_api/sales_orders_lines.csv")
    df_sale_orders = pd.read_excel("files/to_bc_outputs/sales_orders_headers_output.xlsx")
    
    print(df)
    print(getColumns(df))
    print(df_sale_orders)
    
    print(getColumns(df_sale_orders))

    print("......Unique Values......")
    # print(getUniqueFromColumn(df["Document Type"]))
    print("......Unique Values......")


    print("Get Document No.")
    df["Document No."] = df["order_id"].apply(getSecondPositionList)
    print("Get Document Type")
    df["Document Type"] = df["Document No."].apply(getSaleOrderInfo, args=(df_sale_orders,))
    print("Get No.")
    # df["No."] = df["product_id"].apply(getFirstPositionList)
    df["No."] = df["product_id"].apply(getMappingList,args=(bc_ids,"MISSING",))
    print("Location Code")
    df["Location Code"] = df["warehouse_id"].apply(getMappingList,args=(location_code,"",))
    print("Item Type")
    df["Type"] = df["product_type"].apply(getMapping, args=(item_type,"0"))
    print("Generating Line No.")
    # df['Line No.'] = (df.index + 1 ) * 1000
    df['Line No.'] = (df["id"] ) * 1000
    # print(df.index)
    # ,"product_type":"Type"
    # Line No.... duplicados, 10k, 20k, 30k...
    # Type product and service = Item, and False to 0, first letter uppercase


    df.rename(columns={"product_uom_qty":"Quantity","price_unit":"Unit Price","discount":"Line Discount %","x_studio_unit_cost":"Unit Cost"}, inplace=True)

    # print(df)
    getCustomersAndItemsMissing(df)
    # # # Static Columns
    
    df_columns = ["Document Type","Document No.","Line No.","Type","No.","Location Code","Quantity","Unit Price","Line Discount %","Unit Cost","product_id"]
    df = df.reindex(columns=df_columns)
    # print(getColumns(df))

    
    
    print(df)
    df.to_excel("files/to_bc_outputs/sales_orders_lines_output.xlsx",index=False)
    df.to_csv("files/to_bc_outputs/history/sales_orders_lines_output_" + date_time.strftime("%m%d%y_%H%M%S") + ".csv.gz",index=False, compression='gzip')

def getCustomersAndItemsMissing(df):
    # df = pd.read_excel("files/to_bc_outputs/sales_orders_lines_output.xlsx")
    odoo_items = pd.read_csv("files/csvs/from_api/items.csv")
    df = df[(df["No."] == "MISSING") & (df["product_id"] != "False")]["product_id"]
    # print(df)
    unique_list = df.unique()
    
    # print(len(unique_list))
    ids = []
    default_codes = []
    for x in range(0,len(unique_list)):
        ids.append(ast.literal_eval(unique_list[x])[0])
        default_codes.append(ast.literal_eval(unique_list[x])[1])
        
    # print(unique_list)

    df_unique = pd.DataFrame(list(zip(ids,default_codes)), columns=["id","default_code"])
    # print(df_unique)
    df_missing_items = odoo_items[odoo_items['id'].isin(df_unique["id"])]
    df_items_not_found = df_unique[~df_unique['id'].isin(odoo_items["id"])]
    df_missing_items.to_csv("files/validation_files/missing_items_SO_Lines.csv", index=False)
    df_items_not_found.to_csv("files/validation_files/items_not_found_SO_Lines.csv", index=False)

    
    


captureSaleOrderHeaderHeadersDataFromApi()
readSaleOrderHeaderHeadersTransform()

captureSaleOrderLinesDataFromApi()
readSaleOrderLinesTransform()

# getCustomersAndItemsMissing()
