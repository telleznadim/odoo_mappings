import xmlrpc.client
import pandas as pd
import math
from dotenv import dotenv_values

config = dotenv_values(".env") 

def getColumns(df):
    columns = {}
    columns_list_py = []
    columns_list = df.columns
    print(columns_list)
    for col_num in range(0,len(columns_list)):
        columns[col_num] = columns_list[col_num]
        print(columns[col_num].lower())
        columns_list_py.append(columns_list[col_num])
    print(columns_list_py)
    return(columns)

username = config["username"]
password = config["password"]
# info = xmlrpc.client.ServerProxy('captivea-usa-laundrysouth2021-main-1880046/start').start()
# url = "https://laundry-south.odoo.com"
# db = "captivea-usa-laundrysouth2021-main-1880046"
# username = "ntellez@evi-ind.com"
# password = "EVI123"

# common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
# # # print(common.version())
# uid = common.authenticate(db, username, password, {})
# # print(uid)

# models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))
# # ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['is_company', '=', True]]], {'limit': 1})
# # ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[]], {'offset': 10, 'limit': 5})
# # ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[]], { 'limit': 5})
# # ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['customer_rank', '>', 0]]])
# # ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['supplier_rank', '>', 0]]])
# # ids = models.execute_kw(db, uid, password, 'product.product', 'search', [[]])
# # ids = models.execute_kw(db, uid, password, 'product.supplierinfo', 'search', [[]])
# # ids = models.execute_kw(db, uid, password, 'sale.order.line', 'search', [[]])
# # ids = models.execute_kw(db, uid, password, 'stock.valuation.layer', 'search', [[]])
# # ids = models.execute_kw(db, uid, password, 'purchase.order', 'search', [[]])
# # ids = models.execute_kw(db, uid, password, 'account.move', 'search', [[]])
# # info = models.execute_kw(db, uid, password, 'account.aged.payable', 'fields_get', [], {'attributes': ['string', 'help', 'type']})
# # print(info)
# print("IDS")
# ids = models.execute_kw(db, uid, password, 'account.move.line', 'search', [[['move_type','=','entry']]], { 'offset': 8320,'limit':500})

# # move_type = fields.Selection(selection=[
# #             ('entry', 'Journal Entry'),
# #             ('out_invoice', 'Customer Invoice'),
# #             ('out_refund', 'Customer Credit Note'),
# #             ('in_invoice', 'Vendor Bill'),
# #             ('in_refund', 'Vendor Credit Note'),
# #             ('out_receipt', 'Sales Receipt'),
# #             ('in_receipt', 'Purchase Receipt'),
# #         ], string='Type', required=True, store=True, index=True, readonly=True, tracking=True,
# #         default="entry", change_default=True)

# # product = sock.execute_kw(db, uid, password, 'product.template', 'search',[[['barcode','=', str(rec['barcode'])]]])
# print(ids)
# print(len(ids))

# # ids = models.execute_kw(db, uid, password, 'account.move', 'search', [[]], { 'offset': 8320,'limit':500})


# # ids = models.execute_kw(db, uid, password, 'sale.order', 'search', [[['type_name', '=' , 1]]])
# # count = models.execute_kw(db, uid, password, 'sale.order', 'search_count', [[['type_name', '=' , 'quotation']]])
# # count = models.execute_kw(db, uid, password, 'stock.valuation.layer', 'search_count', [[]])
# # print(count)
# # fields = models.execute_kw(db, uid, password, 'sale.order', 'fields_get', [], {'attributes': ['string', 'help', 'type']})
# # print(fields)
# # type_name ==Quotation
# # Sales Order

# # print(ids[5:10])
# # print(len(ids))

# record = models.execute_kw(db, uid, password, 'account.move.line', 'read', [ids])

# # record = models.execute_kw(db, uid, password, 'account.move.line', 'read', [ids], {'fields': ['id', 'name', 'highest_name', 'show_name_warning', 'date', 'ref', 'narration', 'state', 'posted_before', 'move_type', 'type_name', 'to_check', 'journal_id', 'suitable_journal_ids', 'company_id', 'company_currency_id', 'currency_id', 'line_ids', 'partner_id', 'commercial_partner_id', 'country_code', 'user_id', 'is_move_sent', 'partner_bank_id', 'payment_reference', 'payment_id', 'statement_line_id', 'amount_untaxed', 'amount_tax', 'amount_total', 'amount_residual', 'amount_untaxed_signed', 'amount_tax_signed', 'amount_total_signed', 'amount_residual_signed', 'amount_by_group', 'payment_state', 'tax_cash_basis_rec_id', 'tax_cash_basis_move_id', 'auto_post', 'fiscal_position_id', 'invoice_user_id', 'invoice_date', 'invoice_date_due', 'invoice_origin', 'invoice_payment_term_id', 'invoice_line_ids', 'invoice_incoterm_id', 'display_qr_code', 'qr_code_method', 'invoice_outstanding_credits_debits_widget', 'invoice_has_outstanding', 'invoice_payments_widget', 'invoice_vendor_bill_id', 'invoice_source_email', 'invoice_partner_display_name', 'invoice_cash_rounding_id', 'invoice_filter_type_domain', 'bank_partner_id', 'invoice_has_matching_suspense_amount', 'tax_lock_date_message', 'has_reconciled_entries', 'show_reset_to_draft_button', 'restrict_mode_hash_table', 'secure_sequence_number', 'inalterable_hash', 'string_to_hash', 'attachment_ids', 'payment_state_before_switch', 'preferred_payment_method_id', 'edi_document_ids', 'edi_state', 'edi_error_count', 'edi_web_services_to_process', 'edi_show_cancel_button', 'transaction_ids', 'authorized_transaction_ids', 'purchase_vendor_bill_id', 'purchase_id', 'stock_move_id', 'stock_valuation_layer_ids', 'transfer_model_id', 'tax_closing_end_date', 'tax_report_control_error', 'team_id', 'partner_shipping_id', 'purchase_amount', 'asset_id', 'asset_asset_type', 'asset_remaining_value', 'asset_depreciated_value', 'asset_manually_modified', 'asset_value_change', 'asset_ids', 'asset_ids_display_name', 'asset_id_display_name', 'number_asset_ids', 'draft_asset_ids', 'reversal_move_id', 'pay_now', 'is_subscribed', 'is_subscribe_invoice', 'customer_message', 'timesheet_ids', 'timesheet_count', 'order_id', 'campaign_id', 'source_id', 'medium_id', 'activity_ids', 'activity_state', 'activity_user_id', 'activity_type_id', 'activity_type_icon', 'activity_date_deadline', 'my_activity_date_deadline', 'activity_summary', 'activity_exception_decoration', 'activity_exception_icon', 'message_is_follower', 'message_follower_ids', 'message_partner_ids', 'message_channel_ids', 'message_ids', 'message_unread', 'message_unread_counter', 'message_needaction', 'message_needaction_counter', 'message_has_error', 'message_has_error_counter', 'message_attachment_count', 'message_main_attachment_id', 'website_message_ids', 'message_has_sms_error', 'access_url', 'access_token', 'access_warning', 'sequence_prefix', 'sequence_number', 'create_uid', 'create_date', 'write_uid', 'write_date', '__last_update', 'x_studio_subscription_start_date', 'x_studio_quickbooks_reference', 'x_studio_recurring_invoice', 'x_studio_next_invoice_date', 'x_studio_invoice_day_of_month', 'x_studio_invoice_till', 'x_studio_invoice_day_of_month_1', 'x_studio_test', 'x_studio_related_field_Zvp8Y', 'x_studio_related_field_3wZIg', 'x_studio_related_field_hxdUc', 'x_studio_related_field_MZnRx', 'x_studio_related_field_9SOE3', 'x_studio_related_field_sB2oD', 'x_studio_related_field_gnjEH', 'x_studio_po', 'x_studio_related_field_kSNDG', 'x_studio_fiscal_position', 'x_studio_fiscal_position_1', 'x_studio_related_field_dfCJI', 'x_studio_selection_field_pAqJf', 'x_studio_exception', 'x_studio_related_field_4DS1C', 'x_studio_exception_1', 'x_studio_related_field_ymZQT', 'x_studio_related_field_avB9C']})

# # count the number of fields fetched by default
# # print(record)
# # print(len(record))
# df = pd.DataFrame.from_dict(record)

# print(getColumns(df))
# print(df)
# df.to_csv("files/csvs/from_api/account_aged_receivable.csv",index=False)

# offset  8320
# Lenght  10



def captureDataFromApi(steps, table_name):
    url = "https://laundry-south.odoo.com"
    db = "captivea-usa-laundrysouth2021-main-1880046"

    common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
    uid = common.authenticate(db, username, password, {})

    models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))
    # ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['customer_rank', '>', 0]]])
    # ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [[['supplier_rank', '=', 0]]])
    
    records_count = models.execute_kw(db, uid, password, table_name, 'search_count', [[]])
    print(records_count)
    print(records_count/steps)
    limit = steps
    df = pd.DataFrame()
    
    for x in range(0,math.ceil(records_count/limit)):
        offset = x * limit
        print("numbers ",x)
        print("offset ",offset)
        ids = models.execute_kw(db, uid, password, table_name, 'search', [[]], {'offset': offset, 'limit': limit})
        print("Lenght ",len(ids))
        record = models.execute_kw(db, uid, password, table_name, 'read', [ids],{'fields': ['id', 'name', 'highest_name', 'show_name_warning', 'date', 'ref', 'narration', 'state', 'posted_before', 'move_type', 'type_name', 'to_check', 'journal_id', 'suitable_journal_ids', 'company_id', 'company_currency_id', 'currency_id', 'line_ids', 'partner_id', 'commercial_partner_id', 'country_code', 'user_id', 'is_move_sent', 'partner_bank_id', 'payment_reference', 'payment_id', 'statement_line_id', 'amount_untaxed', 'amount_tax', 'amount_total', 'amount_residual', 'amount_untaxed_signed', 'amount_tax_signed', 'amount_total_signed', 'amount_residual_signed', 'amount_by_group', 'payment_state', 'tax_cash_basis_rec_id', 'tax_cash_basis_move_id', 'auto_post', 'fiscal_position_id', 'invoice_user_id', 'invoice_date', 'invoice_date_due', 'invoice_origin', 'invoice_payment_term_id', 'invoice_line_ids', 'invoice_incoterm_id', 'display_qr_code', 'qr_code_method', 'invoice_outstanding_credits_debits_widget', 'invoice_has_outstanding', 'invoice_payments_widget', 'invoice_vendor_bill_id', 'invoice_source_email', 'invoice_partner_display_name', 'invoice_cash_rounding_id', 'invoice_filter_type_domain', 'bank_partner_id', 'invoice_has_matching_suspense_amount', 'tax_lock_date_message', 'has_reconciled_entries', 'show_reset_to_draft_button', 'restrict_mode_hash_table', 'secure_sequence_number', 'inalterable_hash', 'string_to_hash', 'attachment_ids', 'payment_state_before_switch', 'preferred_payment_method_id', 'edi_document_ids', 'edi_state', 'edi_error_count', 'edi_web_services_to_process', 'edi_show_cancel_button', 'transaction_ids', 'authorized_transaction_ids', 'purchase_vendor_bill_id', 'purchase_id', 'stock_move_id', 'stock_valuation_layer_ids', 'transfer_model_id', 'tax_closing_end_date', 'tax_report_control_error', 'team_id', 'partner_shipping_id', 'purchase_amount', 'asset_id', 'asset_asset_type', 'asset_remaining_value', 'asset_depreciated_value', 'asset_manually_modified', 'asset_value_change', 'asset_ids', 'asset_ids_display_name', 'asset_id_display_name', 'number_asset_ids', 'draft_asset_ids', 'reversal_move_id', 'pay_now', 'is_subscribed', 'is_subscribe_invoice', 'customer_message', 'timesheet_ids', 'timesheet_count', 'order_id', 'campaign_id', 'source_id', 'medium_id', 'activity_ids', 'activity_state', 'activity_user_id', 'activity_type_id', 'activity_type_icon', 'activity_date_deadline', 'my_activity_date_deadline', 'activity_summary', 'activity_exception_decoration', 'activity_exception_icon', 'message_is_follower', 'message_follower_ids', 'message_partner_ids', 'message_channel_ids', 'message_ids', 'message_unread', 'message_unread_counter', 'message_needaction', 'message_needaction_counter', 'message_has_error', 'message_has_error_counter', 'message_attachment_count', 'message_main_attachment_id', 'website_message_ids', 'message_has_sms_error', 'access_url', 'access_token', 'access_warning', 'sequence_prefix', 'sequence_number', 'create_uid', 'create_date', 'write_uid', 'write_date', '__last_update', 'x_studio_subscription_start_date', 'x_studio_quickbooks_reference', 'x_studio_recurring_invoice', 'x_studio_next_invoice_date', 'x_studio_invoice_day_of_month', 'x_studio_invoice_till', 'x_studio_invoice_day_of_month_1', 'x_studio_test', 'x_studio_related_field_Zvp8Y', 'x_studio_related_field_3wZIg', 'x_studio_related_field_hxdUc', 'x_studio_related_field_MZnRx', 'x_studio_related_field_9SOE3', 'x_studio_related_field_sB2oD', 'x_studio_related_field_gnjEH', 'x_studio_po', 'x_studio_related_field_kSNDG', 'x_studio_fiscal_position', 'x_studio_fiscal_position_1', 'x_studio_related_field_dfCJI', 'x_studio_selection_field_pAqJf', 'x_studio_exception', 'x_studio_related_field_4DS1C', 'x_studio_exception_1', 'x_studio_related_field_ymZQT', 'x_studio_related_field_avB9C']})
        # record = models.execute_kw(db, uid, password, table_name, 'read', [ids])
        df1 = pd.DataFrame.from_dict(record)
        df = pd.concat([df,df1])

    df.to_csv("files/csvs/from_api/" + table_name + "_all.csv",index=False)


captureDataFromApi(1000,"account.move")


# records_count = models.execute_kw(db, uid, password, 'product.product', 'search_count', [[]])
# print(records_count)
# print(records_count/5000)
# limit = 5000
# df = pd.DataFrame()
# for x in range(0,math.ceil(records_count/limit)):
#     offset = x * limit
#     print("numbers ",x)
#     print("offset ",offset)
#     ids = models.execute_kw(db, uid, password, 'product.product', 'search', [[]], {'offset': offset, 'limit': limit})
#     print("Lenght ",len(ids))
#     record = models.execute_kw(db, uid, password, 'product.product', 'read', [ids])
#     df1 = pd.DataFrame.from_dict(record)
#     df = pd.concat([df,df1])


# print(df)
