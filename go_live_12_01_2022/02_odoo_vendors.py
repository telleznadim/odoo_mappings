import xmlrpc.client
import pandas as pd
from dotenv import dotenv_values
import ast
import re
from datetime import datetime


date_time = datetime.now()

config = dotenv_values(".env")

username = config["username"]
password = config["password"]

states = pd.read_csv("files/relations/contacts/states.csv")
cstvrt = pd.read_csv("files/relations/contacts/CSTVRT.csv")
salesperson = pd.read_csv("files/relations/contacts/salesperson.csv")
intercompany = pd.read_excel(
    "files/relations/vendors/intercompany_vendors.xlsx", skiprows=1)
gen_bus_posting_group = pd.read_csv(
    "files/relations/vendors/gen_bus_posting_group.csv")
vendor_posting_group = pd.read_csv(
    "files/relations/vendors/vendor_posting_group.csv")


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
    ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [
                            [['supplier_rank', '>', 0]]])
    # record = models.execute_kw(db, uid, password, 'res.partner', 'read', [ids], {'fields': ['id', 'display_name','active','street','street2','zip','city','state_id','country_id','email_normalized','phone_sanitized','commercial_company_name','property_payment_term_id',"create_date","__last_update"]})
    record = models.execute_kw(db, uid, password, 'res.partner', 'read', [ids])

    df = pd.DataFrame.from_dict(record)
    del df["image_1920"]
    del df["image_1024"]
    del df["image_512"]
    del df["image_256"]
    del df["image_128"]
    del df["image_medium"]
    print(df)
    df.to_csv("files/csvs/from_api/history/vendors_" +
              date_time.strftime("%m%d%y") + ".csv.gz", index=False, compression='gzip')
    df.to_csv("files/csvs/from_api/vendors.csv", index=False)
    df.to_csv("/Users/nadimtellezbarrera/Library/CloudStorage/OneDrive-EVI/11-LSO/Go Live Simulation/2. Vendors/Complete Master/vendors_" +
              date_time.strftime("%m%d%y") + ".csv", index=False)


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
        if ('(US)' in long_state):
            long_state = long_state[:-5]
            short_state = states.loc[states["long_state"]
                                     == long_state]['short_state'].values[0]

    return (short_state)


def setBlocked(active):
    blocked = ""
    if (not active):
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


def cleanPhoneNo(phone_no):
    return (re.sub('[^0-9 *+*-]', '', phone_no))


def defineIds(vendor_id):
    return ("LSO-" + str(vendor_id).zfill(5))


def getMappingList(old_value, relation_csv, default_value):
    old_value = ast.literal_eval(old_value)

    new_value = default_value
    if (old_value != False):
        find_new = relation_csv.loc[relation_csv["old_value"]
                                    == old_value[0]]['new_value']
        # print(find_new)
        if (not find_new.empty):
            new_value = relation_csv.loc[relation_csv["old_value"]
                                         == old_value[0]]['new_value'].values[0]

    return (new_value)


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
    return (new_value)


def getIntercompanyMapping(old_value, relation_csv, default_value):
    new_value = old_value
    if (new_value != "False"):
        find_new = relation_csv.loc[relation_csv["odoo_id"]
                                    == new_value]['Vendor Type']
        # print(find_new)
        if (not find_new.empty):
            new_value = relation_csv.loc[relation_csv["odoo_id"]
                                         == new_value]['Vendor Type'].values[0]
        else:
            new_value = default_value
    else:
        new_value = default_value
    return (new_value)


def readContactsTransform():
    df = pd.read_csv("files/csvs/from_api/vendors.csv")
    print(df)

    # Get rid of False values
    df['street'] = df['street'].str.replace("False", "")
    df['street2'] = df['street2'].str.replace("False", "")
    df['city'] = df['city'].str.replace("False", "")
    df['email_normalized'] = df['email_normalized'].str.replace("False", "")
    df['commercial_company_name'] = df['commercial_company_name'].str.replace(
        "False", "")

    df["zip"] = df["zip"].apply(cleanPhoneNo)
    df["phone_sanitized"] = df["phone_sanitized"].apply(cleanPhoneNo)
    df["state_id"] = df["state_id"].apply(getStateAbbre)
    df["active"] = df["active"].apply(setBlocked)
    df["country_id"] = df["country_id"].apply(getCountryAbbre)
    df["property_payment_term_id"] = df["property_payment_term_id"].apply(
        getPaymentTerm)
    df["Vendor Type"] = df["id"].apply(
        getIntercompanyMapping, args=(intercompany, "Standard",))

    df["Gen. Bus. Posting Group"] = df["Vendor Type"].apply(
        getMapping, args=(gen_bus_posting_group, "",))
    df["Vendor Posting Group"] = df["Vendor Type"].apply(
        getMapping, args=(vendor_posting_group, "",))

    # df["Gen. Bus. Posting Group"] = "STD"
    # df["Vendor Posting Group"] = "STD"

    df = df.sort_values(by=['Vendor Type', "id"])

    df["No."] = range(60000, 60000+len(df))
    df["No."] = df["No."].apply(defineIds)

    df.rename(columns={"display_name": "Name", "active": "Blocked", "street": "Address", "street2": "Address 2", "zip": "ZIP Code", "city": "City", "state_id": "State", "country_id": "Country/Region Code", "email_normalized": "Email",
              "commercial_company_name": "Name 2", "phone_sanitized": "Phone No.", "property_payment_term_id": "Payment Terms Code", "create_date": "Vendor Since", "__last_update": "Last Date Modified", "id": "odoo_id"}, inplace=True)

    # Static Columns

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

    df["Tax Area Code"] = "STX_"
    df["Tax Liable"] = "True"
    df["Brand"] = ""

    df_columns = ["No.", "Name", "Address", "Address 2", "City", "State", "ZIP Code", "Payment Terms Code", "Contact", "Phone No.", "IRS 1099 Code", "Federal ID No.", "Blocked", "Fax No.", "Email", "Country/Region Code", "Vendor Since", "Legacy Addr 1",
                  "Legacy Addr 2", "Search Name", "Name 2", "Vendor Posting Group", "Gen. Bus. Posting Group", "Our Account No.", "Shipment Method Code", "Shipping Agent Code", "Payment Method Code", "Last Date Modified", "Tax Area Code", "Tax Liable", "Brand", "Vendor Type", "odoo_id"]

    print(getColumns(df))
    df = df.reindex(columns=df_columns)
    print(getColumns(df))

    print(df)

    # print(getColumns(df))
    df.to_excel("files/to_bc_outputs/vendors_output.xlsx", index=False)
    df.to_csv("files/to_bc_outputs/history/vendors_output_" +
              date_time.strftime("%m%d%y_%H%M%S") + ".csvs.gz", index=False, compression='gzip')
    df.to_csv("/Users/nadimtellezbarrera/Library/CloudStorage/OneDrive-EVI/11-LSO/Go Live Simulation/2. Vendors/Download Odoo/vendors_lso_" +
              date_time.strftime("%m%d%y") + ".csv", index=False)
    df.to_excel("/Users/nadimtellezbarrera/Library/CloudStorage/OneDrive-EVI/11-LSO/Go Live Simulation/2. Vendors/Download Odoo/vendors_lso_" +
                date_time.strftime("%m%d%y") + ".xlsx", index=False)


captureDataFromApi()
readContactsTransform()
