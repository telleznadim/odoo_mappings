import xmlrpc.client
import pandas as pd
import numpy as np
from dotenv import dotenv_values
import ast
import re
import math
from datetime import datetime


date_time = datetime.now()

config = dotenv_values(".env")

username = config["username"]
password = config["password"]

df_account_move = pd.read_csv("files/csvs/from_api/account.move.csv")
sales_person = pd.read_csv("files/relations/ar/sales_person.csv")
doc_type = pd.read_csv("files/relations/ap/doc_type.csv")
vendors = pd.read_excel("files/to_bc_outputs/vendors_output.xlsx")


# def captureDataFromApi(steps, table_name):
#     url = "https://laundry-south.odoo.com"
#     db = "captivea-usa-laundrysouth2021-main-1880046"

#     common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
#     uid = common.authenticate(db, username, password, {})

#     models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))
#     # ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['customer_rank', '>', 0]]])
#     # ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['supplier_rank', '=', 0]]])

#     records_count = models.execute_kw(
#         db, uid, password, table_name, 'search_count', [[]])
#     print(records_count)
#     print(records_count/steps)
#     limit = steps
#     df = pd.DataFrame()

#     for x in range(0, math.ceil(records_count/limit)):
#         offset = x * limit
#         print("numbers ", x)
#         print("offset ", offset)
#         ids = models.execute_kw(db, uid, password, table_name, 'search', [
#                                 []], {'offset': offset, 'limit': limit})
#         print("Lenght ", len(ids))
#         record = models.execute_kw(db, uid, password, table_name, 'read', [ids], {'fields': ['id', 'name', 'highest_name', 'show_name_warning', 'date', 'ref', 'narration', 'state', 'posted_before', 'move_type', 'type_name', 'to_check', 'journal_id', 'suitable_journal_ids', 'company_id', 'company_currency_id', 'currency_id', 'line_ids', 'partner_id', 'commercial_partner_id', 'country_code', 'user_id', 'is_move_sent', 'partner_bank_id', 'payment_reference', 'payment_id', 'statement_line_id', 'amount_untaxed', 'amount_tax', 'amount_total', 'amount_residual', 'amount_untaxed_signed', 'amount_tax_signed', 'amount_total_signed', 'amount_residual_signed', 'amount_by_group', 'payment_state', 'tax_cash_basis_rec_id', 'tax_cash_basis_move_id', 'auto_post', 'fiscal_position_id', 'invoice_user_id', 'invoice_date', 'invoice_date_due', 'invoice_origin', 'invoice_payment_term_id', 'invoice_line_ids', 'invoice_incoterm_id', 'display_qr_code', 'qr_code_method', 'invoice_outstanding_credits_debits_widget', 'invoice_has_outstanding', 'invoice_payments_widget', 'invoice_vendor_bill_id', 'invoice_source_email', 'invoice_partner_display_name', 'invoice_cash_rounding_id', 'invoice_filter_type_domain', 'bank_partner_id', 'invoice_has_matching_suspense_amount', 'tax_lock_date_message', 'has_reconciled_entries', 'show_reset_to_draft_button', 'restrict_mode_hash_table', 'secure_sequence_number', 'inalterable_hash', 'string_to_hash', 'attachment_ids', 'payment_state_before_switch', 'preferred_payment_method_id', 'edi_document_ids', 'edi_state', 'edi_error_count', 'edi_web_services_to_process', 'edi_show_cancel_button', 'transaction_ids', 'authorized_transaction_ids', 'purchase_vendor_bill_id', 'purchase_id', 'stock_move_id', 'stock_valuation_layer_ids', 'transfer_model_id', 'tax_closing_end_date', 'tax_report_control_error', 'team_id', 'partner_shipping_id', 'purchase_amount', 'asset_id', 'asset_asset_type',
#                                    'asset_remaining_value', 'asset_depreciated_value', 'asset_manually_modified', 'asset_value_change', 'asset_ids', 'asset_ids_display_name', 'asset_id_display_name', 'number_asset_ids', 'draft_asset_ids', 'reversal_move_id', 'pay_now', 'is_subscribed', 'is_subscribe_invoice', 'customer_message', 'timesheet_ids', 'timesheet_count', 'order_id', 'campaign_id', 'source_id', 'medium_id', 'activity_ids', 'activity_state', 'activity_user_id', 'activity_type_id', 'activity_type_icon', 'activity_date_deadline', 'my_activity_date_deadline', 'activity_summary', 'activity_exception_decoration', 'activity_exception_icon', 'message_is_follower', 'message_follower_ids', 'message_partner_ids', 'message_channel_ids', 'message_ids', 'message_unread', 'message_unread_counter', 'message_needaction', 'message_needaction_counter', 'message_has_error', 'message_has_error_counter', 'message_attachment_count', 'message_main_attachment_id', 'website_message_ids', 'message_has_sms_error', 'access_url', 'access_token', 'access_warning', 'sequence_prefix', 'sequence_number', 'create_uid', 'create_date', 'write_uid', 'write_date', '__last_update', 'x_studio_subscription_start_date', 'x_studio_quickbooks_reference', 'x_studio_recurring_invoice', 'x_studio_next_invoice_date', 'x_studio_invoice_day_of_month', 'x_studio_invoice_till', 'x_studio_invoice_day_of_month_1', 'x_studio_test', 'x_studio_related_field_Zvp8Y', 'x_studio_related_field_3wZIg', 'x_studio_related_field_hxdUc', 'x_studio_related_field_MZnRx', 'x_studio_related_field_9SOE3', 'x_studio_related_field_sB2oD', 'x_studio_related_field_gnjEH', 'x_studio_po', 'x_studio_related_field_kSNDG', 'x_studio_fiscal_position', 'x_studio_fiscal_position_1', 'x_studio_related_field_dfCJI', 'x_studio_selection_field_pAqJf', 'x_studio_exception', 'x_studio_related_field_4DS1C', 'x_studio_exception_1', 'x_studio_related_field_ymZQT', 'x_studio_related_field_avB9C']})
#         # record = models.execute_kw(db, uid, password, table_name, 'read', [ids])
#         df1 = pd.DataFrame.from_dict(record)
#         df = pd.concat([df, df1])

#     df.to_csv("files/csvs/from_api/history/" + table_name + "_" +
#               date_time.strftime("%m%d%y") + "_AP.csv.gz", index=False, compression='gzip')
#     df.to_csv("files/csvs/from_api/" + table_name + "_AP.csv", index=False)
#     df.to_csv("/Users/nadimtellezbarrera/Library/CloudStorage/OneDrive-EVI/11-LSO/Go Live Simulation/9. Open AP/Complete Master/" +
#               table_name + "_AP_" + date_time.strftime("%m%d%y") + ".csv", index=False)


def rewrite_ap_report(file_name):
    df = pd.read_excel(
        "/Users/nadimtellezbarrera/Library/CloudStorage/OneDrive-EVI/11-LSO/Go Live/6. Open AP/1. Download from Odoo/" + file_name)
    columns_ar = df.columns.values
    columns_ar = np.append(columns_ar, "Vendor Name")
    df_final = pd.DataFrame(columns=columns_ar)
    customer_name = ""
    for index, row in df.iterrows():
        if (pd.isna(row["Report Date"])):
            customer_name = row['Unnamed: 0']
        else:
            row["Vendor Name"] = customer_name
            df_final.loc[len(df_final)] = row

    df_final.rename(columns={"Unnamed: 0": "Document No."}, inplace=True)

    df_final["Amount"] = df_final.iloc[:, 5:11].sum(axis=1)
    print(df_final)

    df_final.to_csv(
        "files/relations/ap/aged_payable_transformed.csv", index=False)
    df_final.to_csv("files/relations/ap/history/aged_payable_transformed_" +
                    date_time.strftime("%m%d%y") + ".csv.gz", index=False, compression='gzip')
    df_final.to_csv("/Users/nadimtellezbarrera/Library/CloudStorage/OneDrive-EVI/11-LSO/Go Live/6. Open AP/1. Download from Odoo/aged_payable_transformed_" +
                    date_time.strftime("%m%d%y") + ".csv", index=False)

    # df['Sum']=df.iloc[:,[2,3]].sum(axis=1)


def getUniqueFromColumn(df):
    for item in df.unique():
        print(item)


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


def getFirstPositionList(old_value):
    old_value = ast.literal_eval(old_value)
    if (old_value):
        return (old_value[0])
    else:
        print("old_value", old_value)
        return ("")


def getSecondPositionList(old_value):
    old_value = ast.literal_eval(old_value)
    if (old_value):
        return (old_value[1])
    else:
        return ("")


def searchAccountMove(am_name):
    account_move_info = {"date": "", "type_name": "",
                         "ref": "", "user_id": "", "partner_id": ""}

    if (am_name != "False"):
        find_new = df_account_move.loc[df_account_move["name"] == am_name]
        for value_name in account_move_info:
            if (not find_new[value_name].empty):
                if (not find_new[value_name].isnull().values.any()):
                    account_move_info[value_name] = find_new[value_name].values[0]
            else:
                account_move_info[value_name] = "False"

    # print(account_move_info)
    return (account_move_info)


def searchCustomer(odoo_id, col_name):
    customer_info = {"No.": ""}
    # print(odoo_id)
    if (odoo_id != "False"):
        find_new = vendors.loc[vendors[col_name] == odoo_id]
        for value_name in customer_info:
            if (not find_new[value_name].empty):
                if (not find_new[value_name].isnull().values.any()):
                    customer_info[value_name] = find_new[value_name].values[0]
            else:
                # print(odoo_id)
                customer_info[value_name] = "False_" + col_name

    # print(customer_info)
    return (customer_info)


def setAccountNo(partner_id, customer_name):
    account_no = ""

    # print(partner_id)
    if ((partner_id != False) & (partner_id != "False")):
        partner_id = getFirstPositionList(partner_id)
        account_no = searchCustomer(partner_id, "odoo_id")
    else:
        account_no = searchCustomer(customer_name, "Name")
    return (account_no)


def transformAp(df):
    account_move_info = searchAccountMove(df["Document No."])
    # df["Posting Date"] = account_move_info["date"]
    # df["Document Date"] = account_move_info["date"]

    df["Document Type"] = "Credit Memo" if (
        df["Amount"] >= 0) else "Invoice"

    # df["Document Type"] = getMapping(
    #     account_move_info["type_name"], doc_type, "")
    df["External Document No."] = account_move_info["ref"]
    df["SLEPRS Code (Dimension)"] = getMappingList(
        account_move_info["user_id"], sales_person, "")

    account_no = setAccountNo(
        account_move_info["partner_id"], df["Vendor Name"])
    df["Account No."] = account_no["No."]

    # print(df)
    return (df)


def readApAndTransform():
    df_ap = pd.read_csv("files/relations/ap/aged_payable_transformed.csv")
    print(df_ap)
    print(vendors)
    # getUniqueFromColumn(df_account_move["user_id"])
    # getUniqueFromColumn(df_account_move["type_name"])
    # Invoice
    # Vendor Bill
    # Credit Note
    # Journal Entry
    # Vendor Credit Note
    # Purchase Receipt

    df_ap = df_ap.apply(transformAp, axis=1)

    # df_ap['External Document No.'] = df_ap['External Document No.'].str.replace(
    #     "False", "")
    df_ap["Line No."] = range(10000, (10000*len(df_ap))+1, 10000)
    df_ap["Document No."] = "LSO" + df_ap["Document No."]
    df_ap["External Document No."] = df_ap["Document No."]
    df_ap["Document Date"] = df_ap["Report Date"]

    df_ap["Journal Template Name"] = "PAYMENTS"
    df_ap["Journal Batch Name"] = "DEFAULT"
    df_ap["Account Type"] = "Vendor"
    df_ap["Description"] = ""
    df_ap["Bal. Account No."] = "20000"
    df_ap["Shortcut Dimension 1 Code"] = ""
    df_ap["Gen. Bus. Posting Group"] = ""
    df_ap["Gen. Prod. Posting Group"] = ""
    df_ap["Comment"] = ""
    df_ap["Invoice No."] = ""
    df_ap["EVI BU Code (Dimension)"] = "LSO"
    df_ap["Posting Date"] = ""

    getUniqueFromColumn(df_ap["Document Type"])

    df_columns = ["Journal Template Name", "Journal Batch Name", "Line No.", "Account Type", "Account No.", "Posting Date", "Document Type", "Document No.", "Description", "Bal. Account No.", "Amount",
                  "Shortcut Dimension 1 Code", "Gen. Bus. Posting Group", "Gen. Prod. Posting Group", "Document Date", "External Document No.", "Comment", "Invoice No.", "EVI BU Code (Dimension)", "SLEPRS Code (Dimension)", "Vendor Name"]
    df_ap = df_ap.reindex(columns=df_columns, fill_value="")
    print(df_ap)
    df_ap.to_csv("files/to_bc_outputs/open_ap.csv", index=False)
    df_ap.to_csv("files/to_bc_outputs/history/open_ap_" + date_time.strftime(
        "%m%d%y_%H%M%S") + ".csv.gz", index=False, compression='gzip')
    df_ap.to_csv("/Users/nadimtellezbarrera/Library/CloudStorage/OneDrive-EVI/11-LSO/Go Live/6. Open AP/1. Download from Odoo/open_ap_output_" +
                 date_time.strftime("%m%d%y") + ".csv", index=False)
    df_ap.to_excel("/Users/nadimtellezbarrera/Library/CloudStorage/OneDrive-EVI/11-LSO/Go Live/6. Open AP/1. Download from Odoo/open_ap_output_" +
                   date_time.strftime("%m%d%y") + ".xlsx", index=False)


# captureDataFromApi(1000,"account.move")
# rewrite_ap_report("aged_payable-Odoo-12-01-2022.xlsx")
readApAndTransform()


# Cambiar document Type
# Document No. agregar LSO al inicio
