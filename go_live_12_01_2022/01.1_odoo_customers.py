from lib2to3.pgen2.pgen import DFAState
import xmlrpc.client
from numpy import NAN
import pandas as pd
from dotenv import dotenv_values
import ast
import re
from datetime import datetime

config = dotenv_values(".env")

date_time = datetime.now()
username = config["username"]
password = config["password"]

states = pd.read_csv("files/relations/contacts/states.csv")
cstvrt = pd.read_csv("files/relations/contacts/CSTVRT.csv")
salesperson = pd.read_csv("files/relations/contacts/salesperson.csv")
customer_ids = pd.read_csv("files/relations/customers/ids.csv")
intercompany = pd.read_excel(
    "files/relations/customers/intercompany_customers.xlsx", skiprows=1)
gen_bus_posting_group = pd.read_csv(
    "files/relations/customers/gen_bus_posting_group.csv")
customer_posting_group = pd.read_csv(
    "files/relations/customers/customer_posting_group.csv")


def getColumns(df):
    for col in df.columns:
        print(col)


def captureDataFromApi():
    url = "https://laundry-south.odoo.com"
    db = "captivea-usa-laundrysouth2021-main-1880046"

    common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
    uid = common.authenticate(db, username, password, {})

    models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))
    ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [
                            [['customer_rank', '>', 0]]])
    # ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['supplier_rank', '!=', 0]]])

    # ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[]])
    # record = models.execute_kw(db, uid, password, 'res.partner', 'read', [ids], {'fields': ['id', 'display_name','website','credit_limit','active','street','street2','zip','city','state_id','country_id','email_normalized','mobile','phone_sanitized','commercial_company_name','company_type','property_account_position_id','property_payment_term_id','x_studio_customer_type','user_id',"create_date"]})
    record = models.execute_kw(db, uid, password, 'res.partner', 'read', [ids])

    df = pd.DataFrame.from_dict(record)

    del df["image_1920"]
    del df["image_1024"]
    del df["image_512"]
    del df["image_256"]
    del df["image_128"]
    del df["image_medium"]
    print(df)

    df.to_csv("files/csvs/from_api/history/customers_" +
              date_time.strftime("%m%d%y") + ".csv.gz", index=False, compression='gzip')
    df.to_csv("files/csvs/from_api/customers.csv", index=False)
    df.to_csv("/Users/nadimtellezbarrera/Library/CloudStorage/OneDrive-EVI/11-LSO/Go Live/1. Customers/5. Complete Master/customers_" +
              date_time.strftime("%m%d%y") + ".csv", index=False)


def getUniqueFromColumn(df):
    for item in df.unique():
        print(item)


def getMapping(old_value, relation_csv, default_value):
    new_value = old_value
    if (new_value != "False"):
        find_new = relation_csv.loc[relation_csv["old_value"]
                                    == new_value]['new_value']
        # print(find_new)
        if (not find_new.empty):
            new_value = relation_csv.loc[relation_csv["old_value"]
                                         == new_value]['new_value'].values[0]
        else:
            new_value = default_value
    else:
        new_value = default_value
    return (new_value)


def getStateAbbre(long_state):
    # transform data received
    long_state = ast.literal_eval(long_state)

    # Calculate short State
    short_state = ""
    if (long_state):
        long_state = long_state[1]
        if ('(US)' in long_state):
            long_state = long_state[:-5]
            short_state = states.loc[states["long_state"]
                                     == long_state]['short_state'].values[0]
        # elif('(IN)' in long_state):
        #     print(False)

    # else:
    #     print(2)
    #     print(long_state)

    return (short_state)


def setBlocked(active):
    blocked = ""
    if (not active):
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

    return (short_country)


def getTax(tax_position):
    tax_position = ast.literal_eval(tax_position)
    bc_tax = ""
    if (tax_position):
        tax_position = tax_position[0]
        if (tax_position == 192):
            bc_tax = 99
    return (bc_tax)


def getPaymentTerm(payment_term):
    payment_term = ast.literal_eval(payment_term)

    bc_payment_term_code = "COD"
    if (payment_term):
        payment_term = payment_term[0]

        if (payment_term == 9):  # [9, 'Due on delivery']
            bc_payment_term_code = "COD"
        elif (payment_term == 12):  # [12, 'Credit Card Before Dispatch']
            bc_payment_term_code = "CBS"
        elif (payment_term == 4):  # [4, '30 Days']
            bc_payment_term_code = "NET30"
        elif (payment_term == 2):  # [2, '15 Days']
            bc_payment_term_code = "NET15"
        elif (payment_term == 8):  # [8, '50% Now, Balance on Delivery']
            bc_payment_term_code = "ZZ-REVIEW"

    return (bc_payment_term_code)


def getCustomerVertical(customer_type):
    # transform data received
    # print("IN ",customer_type)
    customer_type = ast.literal_eval(customer_type)

    # Map Customer Vertical
    customer_vertical = ""
    if (customer_type):
        customer_type = customer_type[0]
        customer_vertical = cstvrt.loc[cstvrt["odoo_id"]
                                       == customer_type]['bc_cstvrt'].values[0]

    # print("OUT ",customer_vertical)
    return (customer_vertical)


def getUserId(user_id):
    # print("IN ",user_id)
    user_id = ast.literal_eval(user_id)

    # Map BC User Id
    bc_sale_person = ""
    if (user_id):
        user_id = user_id[0]
        bc_sale_person = salesperson.loc[salesperson["odoo_user_id"]
                                         == user_id]['bc_user_id'].values[0]
    # print("OUT ",bc_sale_person)
    return (bc_sale_person)


def cleanPhoneNo(phone_no):
    phone_no = str(phone_no)
    return (re.sub('[^0-9 *+*-]', '', phone_no))


def defineIds(customer_id):
    return ("LSO-" + str(customer_id).zfill(5))


def modifyIdsRelation(df_new):
    print(customer_ids)
    columns = ["No.", "odoo_id"]
    df_new = df_new.reindex(columns=columns)
    df_new.rename(columns={"No.": "new_value",
                  "odoo_id": "old_value"}, inplace=True)
    df_customers_ids_final = pd.concat([customer_ids, df_new])
    print(df_customers_ids_final)
    df_customers_ids_final.to_csv(
        "files/relations/customers/ids.csv", index=False)


def createFinalDfAddNo(df, last_id):
    df_old = df[df["No."] != "False"]
    df_new = df[df["No."] == "False"]

    if (len(df_new) > 0):
        df_new["No."] = range((last_id), last_id + len(df_new))

        df_new["No."] = df_new["No."].apply(defineIds)
        modifyIdsRelation(df_new)
    print("df_new", df_new)
    df_final = pd.concat([df_old, df_new])
    return (df_final)


def getIntercompanyMapping(old_value, relation_csv, default_value):
    new_value = old_value
    if (new_value != "False"):
        find_new = relation_csv.loc[relation_csv["odoo_id"]
                                    == new_value]['Customer Type']
        # print(find_new)
        if (not find_new.empty):
            new_value = relation_csv.loc[relation_csv["odoo_id"]
                                         == new_value]['Customer Type'].values[0]
        else:
            new_value = default_value
    else:
        new_value = default_value
    return (new_value)


def readContactsTransform():
    df = pd.read_csv("files/csvs/from_api/customers.csv")
    # print(df)
    # parent_true = False
    # df = parentIdFalse(parent_true)

    # Get rid of False values
    df['website'] = df['website'].str.replace("False", "")
    df['street'] = df['street'].str.replace("False", "")
    df['street2'] = df['street2'].str.replace("False", "")
    df['city'] = df['city'].str.replace("False", "")
    df['email_normalized'] = df['email_normalized'].str.replace("False", "")
    df['commercial_company_name'] = df['commercial_company_name'].str.replace(
        "False", "")

    # Transform DATA
    df["zip"] = df["zip"].apply(cleanPhoneNo)
    df["mobile"] = df["mobile"].apply(cleanPhoneNo)
    df["phone_sanitized"] = df["phone_sanitized"].apply(cleanPhoneNo)
    df["state_id"] = df["state_id"].apply(getStateAbbre)
    df["active"] = df["active"].apply(setBlocked)
    df["country_id"] = df["country_id"].apply(getCountryAbbre)
    df['company_type'] = df['company_type'].str.capitalize()
    df["property_account_position_id"] = df["property_account_position_id"].apply(
        getTax)
    df["property_payment_term_id"] = df["property_payment_term_id"].apply(
        getPaymentTerm)

    # getUniqueFromColumn(df["x_studio_customer_type"])
    df["x_studio_customer_type"] = df["x_studio_customer_type"].apply(
        getCustomerVertical)
    df["user_id"] = df["user_id"].apply(getUserId)

    # print(df["country_id"])
    df["Customer Type"] = df["id"].apply(
        getIntercompanyMapping, args=(intercompany, "Standard",))
    df = df.sort_values(by=['Customer Type', "id"])
    df["Gen. Bus. Posting Group"] = df["Customer Type"].apply(
        getMapping, args=(gen_bus_posting_group, "",))
    df["Customer Posting Group"] = df["Customer Type"].apply(
        getMapping, args=(customer_posting_group, "",))
    # df["Customer Posting Group"] = "STD"
    # df["Gen. Bus. Posting Group"] = "STD"

    df["No."] = df["id"].apply(getMapping, args=(customer_ids, "False",))
    # df["No."] = range(1, 1+len(df))
    # df["No."] = df["No."].apply(defineIds)

    df.rename(columns={"display_name": "Name", "website": "Home Page", "credit_limit": "Credit Limit ($)", "active": "Blocked", "street": "Address", "street2": "Address 2", "zip": "ZIP Code", "city": "City", "state_id": "State", "country_id": "Country/Region Code", "mobile": "Mobile Phone No.", "company_type": "Partner Type", "email_normalized": "Email",
              "commercial_company_name": "Name 2", "phone_sanitized": "Phone No.", "property_account_position_id": "SureTax© Exemption Code", "x_studio_customer_type": "Customer Vertical", "property_payment_term_id": "Payment Terms Code", "user_id": "Salesperson Code", "create_date": "Customer Since", "id": "odoo_id"}, inplace=True)

    # Static Columns

    df["Bill-to Customer No."] = df["No."]
    df["Location Code"] = "WH1"
    df["Tax Area Code"] = "STX_"
    df["Contact"] = ""
    df["Territory Code"] = ""
    df["Shipment Method Code"] = "GROUND"
    df["Customer Disc. Group"] = ""
    df["Fax No."] = ""
    df["EORI Number"] = ""
    df["Tax Liable"] = "True"
    df["Purchase Order Required"] = ""
    df["Legacy Note ID"] = ""
    df["Legacy Addr 1"] = ""
    df["Legacy Addr 2"] = ""

    customer_columns = ["No.", "Customer Since", "Name", "Home Page", "Credit Limit ($)", "Blocked", "Address", "Address 2", "ZIP Code", "City", "State", "Country/Region Code", "Mobile Phone No.", "Partner Type", "Name 2", "Email", "Phone No.", "SureTax© Exemption Code", "Customer Vertical", "Payment Terms Code", "Salesperson Code",
                        "Customer Posting Group", "Gen. Bus. Posting Group", "Bill-to Customer No.", "Location Code", "Tax Area Code", "Contact", "Territory Code", "Shipment Method Code", "Customer Disc. Group", "Fax No.", "EORI Number", "Tax Liable", "Purchase Order Required", "Legacy Note ID", "Legacy Addr 1", "Legacy Addr 2", "Customer Type", "odoo_id"]
    df = df.reindex(columns=customer_columns)
    print(getColumns(df))
    print(df)
    df = createFinalDfAddNo(df, len(customer_ids) +
                            50000).sort_values(by="No.")
    print(df)

    # print(df[df["No."] == "False"])

    # print(getColumns(df))
    df.to_excel("files/to_bc_outputs/customers_output.xlsx", index=False)
    # df.to_excel("files/to_bc_outputs/customers_output_" + str(parent_true) + ".xlsx",index=False)
    df.to_csv("files/to_bc_outputs/history/customers_output_" +
              date_time.strftime("%m%d%y_%H%M%S") + ".csvs.gz", index=False, compression='gzip')
    df.to_csv("/Users/nadimtellezbarrera/Library/CloudStorage/OneDrive-EVI/11-LSO/Go Live/1. Customers/1. Download from Odoo/customers_lso_" +
              date_time.strftime("%m%d%y") + ".csv", index=False)
    df.to_excel("/Users/nadimtellezbarrera/Library/CloudStorage/OneDrive-EVI/11-LSO/Go Live/1. Customers/1. Download from Odoo/customers_lso_" +
                date_time.strftime("%m%d%y") + ".xlsx", index=False)


def parentIdFalse(parent_true):
    df = pd.read_csv("files/csvs/from_api/customers.csv")
    df_true = df[df["parent_id"] != "False"]
    df_false = df[df["parent_id"] == "False"]
    print(df_false)
    print(df_true)
    df_true.to_csv(
        "files/csvs/from_api/customers_parent_id_not_false.csv", index=False)
    df_false.to_csv(
        "files/csvs/from_api/customers_parent_id_false.csv", index=False)
    if (parent_true):
        return (df_false)
    else:
        return (df_true)


# captureDataFromApi()
readContactsTransform()
# parentIdFalse()
