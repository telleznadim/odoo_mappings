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
# location_code = pd.read_csv("files/relations/sale_order/location_code.csv")
# document_type = pd.read_csv("files/relations/sale_order/document_type.csv")
assigned_person = pd.read_csv("files/relations/purchase_order/assigned_person.csv")
# payment_term = pd.read_csv("files/relations/sale_order/payment_term.csv")
vendors = pd.read_excel("files/to_bc_outputs/vendors_output.xlsx")
customers = pd.read_excel("files/to_bc_outputs/customers_output.xlsx")
item_type = pd.read_csv("files/relations/purchase_order/item_type.csv")


def getColumns(df):
    for col in df.columns:
        print(col)


def capturePurchaseOrderHeaderHeadersDataFromApi():
    url = "https://laundry-south.odoo.com"
    db = "captivea-usa-laundrysouth2021-main-1880046"

    common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
    uid = common.authenticate(db, username, password, {})

    models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))
    # ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['customer_rank', '>', 0]]])
    # ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['supplier_rank', '=', 0]]])
    
    records_count = models.execute_kw(db, uid, password, 'purchase.order', 'search_count', [[]])
    print(records_count)
    print(records_count/5000)
    limit = 5000
    df = pd.DataFrame()
    
    for x in range(0,math.ceil(records_count/limit)):
        offset = x * limit
        print("numbers ",x)
        print("offset ",offset)
        ids = models.execute_kw(db, uid, password, 'purchase.order', 'search', [[]], {'offset': offset, 'limit': limit})
        print("Lenght ",len(ids))
        # record = models.execute_kw(db, uid, password, 'purchase.order', 'read', [ids],{'fields': ["name","warehouse_id","date_order","expected_date","partner_id","x_studio_customer_po","user_id","x_studio_delivery_status","payment_term_id","activity_summary","invoice_ids","partner_id","partner_shipping_id","type_name","origin"]})
        record = models.execute_kw(db, uid, password, 'purchase.order', 'read', [ids])
        df1 = pd.DataFrame.from_dict(record)
        df = pd.concat([df,df1])

    df.to_csv("files/csvs/from_api/purchase_orders.csv",index=False)
    df.to_csv("files/csvs/from_api/history/purchase_orders_" + date_time.strftime("%m%d%y_%H%M%S") + ".csv.gz",index=False, compression='gzip')


def capturePurchaseOrderLinesDataFromApi():
    url = "https://laundry-south.odoo.com"
    db = "captivea-usa-laundrysouth2021-main-1880046"

    common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
    uid = common.authenticate(db, username, password, {})

    models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))
    # ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['customer_rank', '>', 0]]])
    # ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['supplier_rank', '=', 0]]])
    
    records_count = models.execute_kw(db, uid, password, 'purchase.order.line', 'search_count', [[]])
    print(records_count)
    print(records_count/5000)
    limit = 5000
    df = pd.DataFrame()
    
    for x in range(0,math.ceil(records_count/limit)):
        offset = x * limit
        print("numbers ",x)
        print("offset ",offset)
        ids = models.execute_kw(db, uid, password, 'purchase.order.line', 'search', [[]], {'offset': offset, 'limit': limit})
        print("Lenght ",len(ids))
        record = models.execute_kw(db, uid, password, 'purchase.order.line', 'read', [ids])
        df1 = pd.DataFrame.from_dict(record)
        df = pd.concat([df,df1])

    df.to_csv("files/csvs/from_api/purchase_orders_lines.csv",index=False)
    df.to_csv("files/csvs/from_api/history/purchase_orders_lines_" + date_time.strftime("%m%d%y_%H%M%S") + ".csv.gz",index=False, compression='gzip')
    

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

def getPurchaseOrder(df,purchase_orders_list):
    purchase_order_id = getFirstPositionList(df["order_id"])
    po_info = getPOInfo(purchase_order_id,purchase_orders_list)
    df["Document Type"] = po_info["Document Type"]
    df["Document No."] = po_info["No."]
    return(df)

def getPOInfo(purchase_order_id, purchase_orders_list):
    po_info = {"Document Type" : "", "No." : ""}
    # print(sale_order_id)

    if (purchase_order_id != "False"):
        # Protecting the search to adding more conditions when searching....
        find_new = purchase_orders_list.loc[purchase_orders_list["id"] == purchase_order_id]
        for value_name in po_info:
            if (not find_new[value_name].empty):
                if (not find_new[value_name].isnull().values.any()):
                    po_info[value_name] = find_new[value_name].values[0]
            else:
                po_info[value_name] = "Missing Order No."
    
    return(po_info)



# getBillInfo(2835)

def getBillAddress(df):
    # print(df["dest_address_id"])
    customer_shipp_id = ast.literal_eval(df["dest_address_id"])

    if(customer_shipp_id):
        ship_info = getShipInfo(customer_shipp_id[0])
        df["Ship-to Name"] = ship_info["Name"]
        df["Ship-to Address"] = ship_info["Address"]
        df["Ship-to Address 2"] = ship_info["Address 2"] 
        df["Ship-to City"] = ship_info["City"] 
        df["Ship-to State"] = ship_info["State"]
        df["Ship-to ZIP Code"] = ship_info["ZIP Code"]
    else:
        df["Ship-to Name"] = ""
        df["Ship-to Address"] = ""
        df["Ship-to Address 2"] = ""
        df["Ship-to City"] = ""
        df["Ship-to State"] = ""
        df["Ship-to ZIP Code"] = ""

    return(df)

def readPurchaseOrderHeaderTransform():
    df = pd.read_csv("files/csvs/from_api/purchase_orders.csv")
    print(df)
    # print(vendors)
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
    df['Internal Comment'] = df['x_studio_shipping_notes'].str.replace("False","")
    # del df['expected_date']
    # df['External Document No.'] = df['x_studio_customer_po'].str.replace("False","")
    # del df["x_studio_customer_po"]
    # df["Shipment Method Code"] = df["x_studio_delivery_status"].str.replace("False","")
    # del df["x_studio_delivery_status"]
    # df["Work Description"] = df["activity_summary"].str.replace("False","")
    # del df["activity_summary"]
    # df["Your Reference"] = df["origin"].str.replace("False","")
    # del df["origin"]


    
    # # Transform DATA
    # print("Location Code")
    # df["Location Code"] = df["warehouse_id"].apply(getMappingList,args=(location_code,"",))
    # del df['warehouse_id']
    # print("Document Type")
    # df["Document Type"] = df["type_name"].apply(getMapping,args=(document_type,))
    # del df['type_name']
    print("Buy-from Vendor No.")
    df["Buy-from Vendor No."] = df["partner_id"].apply(getFirstPositionList)
    print("Getting Sales Person Code")
    df["Assigned User ID"] = df["user_id"].apply(getMappingList,args=(assigned_person,"",))
    # del df['user_id']
    # print("Getting Payment Terms Code")
    # df["Payment Terms Code"] = df["payment_term_id"].apply(getMappingList,args=(payment_term,"COD",))
    # del df['payment_term_id']
    print("Getting Ship-to Info")
    df = df.apply(getBillAddress, axis=1)
    print(df)
   
    df.rename(columns={"name": "No.","create_date":"Document Date","date_order":"Order Date"}, inplace=True)
    

    # # # Static Columns
    df["Location Code"] = ""
    df["Document Type"] = "Order"
    df["Shipment Method Code"] = ""
    df["Ship-to Contact"] = ""
    df["Payment Terms Code"] = ""
    
    df_no_mapping_columns = ["id","No.","Document Type"]
    df_no_mapping = df.reindex(columns=df_no_mapping_columns)

    # df_columns = ["No.","Document Date","Order Date","Shipment Date","Document Type","Sell-to Customer No.","External Document No.","Salesperson Code","Shipment Method Code","Payment Terms Code","Work Description","Invoice","Address","Address 2","City","State","Zip Code","Ship-to Name","Ship-to Address","Ship-to Address 2","Ship-to City","Ship-to State","Ship-to ZIP Code","Your Reference","Document Date"]
    df_columns = ["No.","Location Code","Document Date", "Buy-from Vendor No.","Document Type","Internal Comment","Ship-to Name","Ship-to Address","Ship-to Address 2","Ship-to City","Ship-to State","Ship-to ZIP Code","Shipment Method Code","Ship-to Contact","Payment Terms Code","Assigned User ID","Order Date"]
    

    df = df.reindex(columns=df_columns)
    # print(getColumns(df))
    print(df)
    
    
    # print(getColumns(df))
    # df.to_excel("files/to_bc_outputs/customers2_output.xlsx",index=False)
    # df.to_excel("files/to_bc_outputs/contacts_output.xlsx",index=False)
    df_no_mapping.to_csv("files/relations/purchase_order/po_ids.csv",index=False)
    df.to_csv("files/to_bc_outputs/history/purchase_orders_headers_output_" + date_time.strftime("%m%d%y_%H%M%S") + ".csv",index=False, compression='gzip')
    df.to_excel("files/to_bc_outputs/purchase_orders_headers_output.xlsx",index=False)




def readPurchaseOrderLinesTransform():
    df = pd.read_csv("files/csvs/from_api/purchase_orders_lines.csv")
    df_purchase_orders = pd.read_excel("files/to_bc_outputs/purchase_orders_headers_output.xlsx")
    df_no_mapping = pd.read_csv("files/relations/purchase_order/po_ids.csv")
    
    getUniqueFromColumn(df["product_type"])
    print(df)
    print(getColumns(df))
    print(df_purchase_orders)
    print(getColumns(df_purchase_orders))


    print("......Unique Values......")
    print(getUniqueFromColumn(df["product_type"]))
    print("......Unique Values......")


    print("Get Document No.") #order_id
    df = df.apply(getPurchaseOrder, args=(df_no_mapping,), axis=1)



    print("Get Product No.")
    df["No."] = df["product_id"].apply(getMappingList,args=(bc_ids,"MISSING",))
    df["Unit Cost ($)"] = df["price_unit"]
    df["Direct Unit Cost"] = df["price_unit"]
    print("Item Type")
    df["Type"] = df["product_type"].apply(getMapping, args=(item_type,"0"))
    print("Generating Line No.")
    df['Line No.'] = (df["id"]) * 1000
    # print("Location Code")
    # df["Location Code"] = df["warehouse_id"].apply(getMappingList,args=(location_code,"",))

    df.rename(columns={"product_qty":"Quantity","x_studio_unit_cost":"Unit Cost","date_planned":"Expected Receipt Date"}, inplace=True)

    # # # Static Columns
    df["Location Code"] = ""
    getVendorsAndItemsMissing(df)
    
    df_columns = ["Document Type","Document No.","Line No.","Type","No.","Location Code","Expected Receipt Date","Quantity","Unit Cost ($)","Direct Unit Cost","product_id"]
    df = df.reindex(columns=df_columns)
    # print(getColumns(df))

    
    
    print(df)
    df.to_excel("files/to_bc_outputs/purchase_orders_lines_output.xlsx",index=False)
    df.to_csv("files/to_bc_outputs/history/purchase_orders_lines_output_" + date_time.strftime("%m%d%y_%H%M%S") + ".csv.gz",index=False, compression='gzip')
    
    
def getVendorsAndItemsMissing(df):
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
    df_missing_items.to_csv("files/validation_files/missing_items_PO_Lines.csv", index=False)
    df_items_not_found.to_csv("files/validation_files/items_not_found_PO_Lines.csv", index=False)
    

# capturePurchaseOrderHeaderHeadersDataFromApi()
# readPurchaseOrderHeaderTransform()

# capturePurchaseOrderLinesDataFromApi()
readPurchaseOrderLinesTransform()
