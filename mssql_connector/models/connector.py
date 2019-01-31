# -*- coding: utf-8 -*-

from odoo import fields, models, api, tools, _
from odoo.tools import DEFAULT_SERVER_DATETIME_FORMAT,DEFAULT_SERVER_DATE_FORMAT
from odoo.exceptions import UserError
from datetime import datetime
import pymssql
import logging


class MSSQLConnectorLog(models.Model):
    _name = 'mssql.connector.log'
    description = "to log failed transaction to mssql"

    date = fields.Datetime(string='Captured On', copy=False, default=fields.Datetime.now)
    log = fields.Text(string="Log")
    db_name = fields.Char(string='Database', required=True)
    model_name = fields.Char(string='Model', required=True)
    connector_id =  fields.Many2one('mssql.connector', string='Connector', required=True)

MSSQLConnectorLog()


class MSSQLConnector(models.Model):
    _name = 'mssql.connector'
    description = "to handle configurations and fetch data from mssql"

    name = fields.Char(string='Name')
    active = fields.Boolean(default=True, string='Active')
    host = fields.Char(string='Host', required=True)
    username = fields.Char(string='Username', required=True)
    password = fields.Char(string='Password', required=True)
    db_name = fields.Char(string='Database', required=True)
    limit = fields.Integer(default=500, string='Limit', required=True)
    invoice_model = fields.Char(string='Invoice Model')
    payment_model = fields.Char(string='Payment Model')
    internal_payment_model = fields.Char(string='Internal Payment Model')    


    @api.multi
    def name_get(self):
        res = []
        for connector in self:
            name = '%s - %s' % (connector.db_name, connector.host)
            res.append((connector.id, name))
        return res


    @api.multi
    def test_mssql_connection(self):
        for connector in self:
            connection = False
            try:
                connection = pymssql.connect(connector.host, connector.username, connector.password, connector.db_name)
            except Exception as e:
                raise UserError(_("Connection Test Failed! Here is what we got instead:\n\n %s") % (e))
            finally:
                try:
                    if connection:
                        conn.close()
                except Exception:
                    pass
        raise UserError(_("Connection Test Succeeded! Everything seems properly set up!"))


    def get_invoice_data(self, data):
        '''
            method to get invoice values
            returns dict with keys invoice_data or error_msg
        '''
        if not data:
            return {'error_msg':'Data not found !'}

        invoice_data = {}
        line_data ={}
        company =  False
        product = False
        try:
            # identify company_id for invoice
            if data.get('COMPANY_ID'):
                invoice_data['company_id'] = data.get('COMPANY_ID')
                company = self.env['res.company'].sudo().browse([invoice_data.get('company_id')])
                if not company:
                    return {'error_msg':'Invalid Company'}
            else:
                return {'error_msg':'Invalid Company'}

            # identify invoice type
            if data.get('IS_SALES_INVOICE'):
                invoice_data['type'] = 'out_invoice'
                if data.get('PRICE', 0.0) < 0:
                    invoice_data['type'] = 'out_refund'
                invoice_data['x_studio_clinicid'] = data.get('CLINICID', 0)
                invoice_data['x_studio_soid_2'] = data.get('SOID', '')
                invoice_data['x_studio_custid'] = data.get('CUSTID', 0)
            else:
                invoice_data['type'] = 'in_invoice'
                if data.get('PRICE', 0.0) < 0:
                    invoice_data['type'] = 'in_refund'
                invoice_data['x_studio_clinicid_1'] = data.get('CLINICID', 0)
                invoice_data['x_studio_soid_3'] = data.get('SOID', '')
                invoice_data['x_studio_custid_1'] = data.get('CUSTID', 0)

            # get partner, account_id, payment_term_id for invoice,
            if data.get('PARTNER_ID') and company:
                partner = self.env['res.partner'].sudo().search([('id', '=', data.get('PARTNER_ID'))])
                if partner:
                    partner = partner.with_context(force_company=company.id)
                    invoice_data['partner_id'] = partner.id
                    account_id = payment_term_id =False
                    if invoice_data.get('type') in ('out_invoice', 'out_refund'):
                        account_id = partner.property_account_receivable_id
                        payment_term_id = partner.property_payment_term_id
                    else:
                        account_id = partner.property_account_payable_id
                        payment_term_id = partner.property_supplier_payment_term_id
                    if account_id:
                        invoice_data['account_id'] = account_id.id
                    if payment_term_id:
                        invoice_data['payment_term_id'] = payment_term_id.id
                else:
                    return {'error_msg':'Invalid Partner with company'}
            else:
                return {'error_msg':'Partner not found'}

            #get origin
            if data.get('TRANS_ID'):
                invoice_data['origin'] = data.get('TRANS_ID')
            else:
                return {'error_msg':'TRANS_ID not found'}

            #get date_invoice
            if data.get('DATE_INVOICE'):
                date_invoice = datetime.strptime(data.get('DATE_INVOICE'),"%Y-%m-%d").date()
                invoice_data['date_invoice'] = date_invoice.strftime(DEFAULT_SERVER_DATE_FORMAT)

            #get currency_id
            if data.get('CURRENCY_ID'):
                invoice_data['currency_id'] = data.get('CURRENCY_ID')
            else:
                return {'error_msg':'Invalid CURRENCY_ID'}

            if data.get('TRANS_REF'):
                invoice_data['name'] = data.get('TRANS_REF')
            else:
                return {'error_msg':'TRANS_REF Missing'}

            # get product_id for line
            if data.get('PRODUCT_ID'):
                product = self.env['product.product'].sudo().search([('id','=',data.get('PRODUCT_ID')), ('company_id','=',company.id)])
                if product:
                    line_data['product_id'] = product.id
                    if product.uom_id:
                        line_data['uom_id'] =  product.uom_id.id
                else:
                    return {'error_msg':'Invalid product with company'}
            else:
                return {'error_msg':'Invalid product'}

            # get journal, account, taxes for invoice line
            if company and invoice_data.get('type') and product:
                journal = self.env['account.invoice'].with_context({'company_id':company.id,'type':invoice_data.get('type')}).sudo()._default_journal()
                if journal:
                    invoice_data['journal_id'] = journal.id
                account = self.env['account.invoice.line'].sudo().get_invoice_line_account(invoice_data.get('type'), product, None, company)
                if account:
                    line_data['account_id'] = account.id

            line_data['name'] = data.get('DESCRIPTION', '/')

            if data.get('QTY'):
                line_data['quantity'] = data.get('QTY', 1)

            if data.get('PRICE'):
                line_data['price_unit'] = abs(data.get('PRICE', 0.0))

            # currency conversion
#            if invoice_data.get('currency_id') and data.get('PRICE') and invoice_data.get('date_invoice'):
#                currency = self.env['res.currency'].browse([invoice_data.get('currency_id')])
#                if company.currency_id != currency:
#                    currency_rate = currency.with_context({'date':invoice_data.get('date_invoice')}).rate
#                    if currency_rate != data.get('CURRENCY_RATE'):
#                        currency_rate = data.get('CURRENCY_RATE')
#                    line_data['price_unit'] = data.get('PRICE', 0.0) * currency_rate

            if line_data:
                invoice_data['invoice_line_ids'] = [(0, 0, line_data)]

        except Exception as e:
            return {'error_msg':'%s:%s' %(data.get('TRANS_ID'), e)}


        return {'invoice_data':invoice_data}

    @api.multi
    def register_log(self, model='', msg=False):
        '''
           method to log error on query execution
        '''
        self.ensure_one()
        logging.error((self,model,msg))
        if msg:
            self.env['mssql.connector.log'].create({
                                        'connector_id':self.id,
                                        'db_name':self.db_name,
                                        'model_name': model,
                                        'log':msg,
                                    })
        return True

    @api.model
    def execute_update_query(self, conn, cursor, query, trans_id, model):
        '''
           method for update query execution
        '''
        try:
            if conn and cursor and query:
                cursor.execute(query)
                conn.commit()
        except Exception as e:
            self.register_log(model=model, msg='ID :%s,  Msg: %s, Query: %s' %(trans_id, e, query))

    @api.multi
    def run_connector(self):
        self.ensure_one()
        if self.invoice_model:
            self.run_connector_invoice()
        if self.payment_model:
            self.run_connector_payment()
        return True

    @api.multi
    def run_connector_invoice(self):
        '''
           method to connect mssql and create invoices
        '''
        InvoiceObj = self.env['account.invoice']
        CurrencyRateObj = self.env['res.currency.rate']
        for connector in self.sudo():
            connection = False
            if not connector.invoice_model:
                if self._context.get('raise_error'):
                    raise UserError(_("Please check the Invoice Model"))
                connector.register_log(model='Not Defined', msg="Please check the Invoice Model")
                continue
            try:
                connection = pymssql.connect(connector.host, connector.username, connector.password, connector.db_name)
                cursor = connection.cursor(as_dict=True)
                select_query = 'SELECT TOP %s * FROM %s WHERE ODOO_IS_READ=0' %(connector.limit, connector.invoice_model)
                cursor.execute(select_query)
                cursor_data = cursor.fetchall()
            except Exception as e:
                if self._context.get('raise_error'):
                    raise UserError(_("Connection Failed! Here is what we got instead:\n\n %s") % (e))
                else:
                    connector.register_log(model=connector.invoice_model, msg="Connection Failed! Here is what we got instead:\n\n %s" % (e))
                    continue

            if cursor_data and not 'TRANS_ID' in cursor_data[0].keys():
                warning = "TRANS_ID not found in the model %s. Please check the Invoice model" %(connector.invoice_model)
                if self._context.get('raise_error'):
                    raise UserError(_(warning))
                connector.register_log(model=connector.invoice_model, msg=warning)
                continue

            company_data = {}
            for data in cursor_data:
                if data.get('COMPANY_ID') not in company_data.keys():
                    company_data[data.get('COMPANY_ID')] = [data]
                else:
                    company_data[data.get('COMPANY_ID')].append(data)

            for company_id, data_value in company_data.items():
                if connector.env.user.company_id.id != company_id:
                    connector.env.user.company_id = company_id
                for data in data_value:
                    if data.get('PRICE', 0.0) == 0:
                        try:
                            date_time_now = fields.Datetime.now()
                            vals = (connector.invoice_model, date_time_now, data.get('TRANS_ID'))
                            update_query = "UPDATE %s set ODOO_IS_READ=1, ODOO_IS_READ_ON='%s' where TRANS_ID=%s" %vals
                            connector.execute_update_query(connection, cursor, update_query, data.get('TRANS_ID'), connector.invoice_model)
                        except Exception as e:
                            msg = e and str(e).replace("'","")
                            vals = (connector.invoice_model, msg, data.get('TRANS_ID'))
                            update_query  = "UPDATE %s set ODOO_READ_SUCCESS=0, ODOO_ERROR_MESSAGE='%s' where TRANS_ID=%s" %vals
                            connector.execute_update_query(connection, cursor, update_query, data.get('TRANS_ID'), connector.invoice_model)
                            connector.register_log(model=connector.invoice_model, msg='TRANS_ID :%s \nMsg: %s \n\nData: %s' %(data.get('TRANS_ID'), msg, data))
                        continue
                    invoice_data = connector.get_invoice_data(data)
                    invoice_vals = invoice_data.get('invoice_data', False)
                    if invoice_data.get('error_msg', False) and data.get('TRANS_ID'):
                        vals = (connector.invoice_model, invoice_data.get('error_msg'), data.get('TRANS_ID'))
                        update_query  = "UPDATE %s set ODOO_READ_SUCCESS=0, ODOO_ERROR_MESSAGE='%s' where TRANS_ID=%s" %vals
                        connector.execute_update_query(connection, cursor, update_query, data.get('TRANS_ID'), connector.invoice_model)
                        connector.register_log(model=connector.invoice_model, msg='TRANS_ID :%s \nMsg: %s \n\nData: %s' %(data.get('TRANS_ID'), invoice_data.get('error_msg'), data))
                    elif invoice_vals and data.get('TRANS_ID'):
                        try:
                            invoice = InvoiceObj.sudo().create(invoice_vals)
                            if data.get('CURRENCY_RATE'):
                                currency_rate = invoice.sudo().currency_id.rate_ids.filtered(lambda rec: rec.company_id and \
                                            rec.company_id.id==invoice.company_id.id and rec.name==invoice.date_invoice)
                                if currency_rate and currency_rate.rate != data.get('CURRENCY_RATE'):
                                    currency_rate.rate = data.get('CURRENCY_RATE')
                                elif not currency_rate:
                                    CurrencyRateObj.sudo().create({
                                                            'name':invoice.date_invoice,
                                                            'currency_id':invoice.currency_id.id,
                                                            'company_id':invoice.company_id.id,
                                                            'rate':data.get('CURRENCY_RATE')
                                                        })
                            duplicate_invoices = self.env['account.invoice'].sudo().search([('number','=',data.get('TRANS_REF')),('company_id','=',invoice.company_id.id),
                                                                                             ('journal_id','=',invoice.journal_id.id),('type','=',invoice.type)])
                            if duplicate_invoices:
                                vals = (connector.invoice_model, 'Duplicate TRANS_REF', data.get('TRANS_ID'))
                                update_query  = "UPDATE %s set ODOO_READ_SUCCESS=0, ODOO_ERROR_MESSAGE='%s' where TRANS_ID=%s" %vals
                                connector.execute_update_query(connection, cursor, update_query, data.get('TRANS_ID'), connector.invoice_model)
                                connector.register_log(model=connector.invoice_model, msg='TRANS_ID :%s \nMsg: Duplicate TRANS_REF \n\n Data: %s' %(data.get('TRANS_ID'), data))
                                invoice.action_cancel()
                            else:
                                invoice.with_context({'TRANS_REF':data.get('TRANS_REF')}).sudo().action_invoice_open()
                                date_time_now = fields.Datetime.now()
                                values = (connector.invoice_model, date_time_now, invoice.move_id.name, data.get('TRANS_ID'))
                                success_query = "UPDATE %s set ODOO_READ_SUCCESS=1, ODOO_IS_READ=1, ODOO_IS_READ_ON='%s', ODOO_JOURNAL_REF='%s',ODOO_ERROR_MESSAGE='' where TRANS_ID=%s;" %values
                                connector.execute_update_query(connection, cursor, success_query, data.get('TRANS_ID'), connector.invoice_model)
                        except Exception as e:
                            logging.error(e)
                            msg = e and str(e).replace("'","")
                            vals = (connector.invoice_model, msg, data.get('TRANS_ID'))
                            update_query  = "UPDATE %s set ODOO_READ_SUCCESS=0, ODOO_ERROR_MESSAGE='%s' where TRANS_ID=%s" %vals
                            connector.execute_update_query(connection, cursor, update_query, data.get('TRANS_ID'), connector.invoice_model)
                            connector.register_log(model=connector.invoice_model, msg='TRANS_ID :%s \nMsg: %s \n\n Data: %s' %(data.get('TRANS_ID'), msg, data))
            if connection:
                connection.close()
        return True

    @api.multi
    def run_mssql_connector_invoice_cron(self):
        '''
            cron method to pull data for invoices
        '''
        for connector in self.env['mssql.connector'].sudo().search([]):
            connector.sudo().run_connector_invoice()
        return True

    def get_payment_data(self, data):
        """
        Get the payment values

        :rtype           : `dict`
        :returns         : dictionary with keys payment_data or error_msg

        """
        if not data:
            return {'error_msg': 'Data not found !'}

        payment_data = {}
        company = False
        payment_method = False
        try:
            # Company
            if data.get('COMPANY_ID'):
                payment_data['company_id'] = data.get('COMPANY_ID')
                company = self.env['res.company'].sudo().browse([payment_data.get('company_id')])
                if not company:
                    return {'error_msg':'Invalid Company'}
            else:
                return {'error_msg':'Invalid Company'}
            # Payment Type
            if data.get('IS_PAYMENT'):
                payment_data['payment_type'] = 'outbound'
                payment_data['partner_type'] = 'supplier'
                payment_method = self.env.ref('account.account_payment_method_manual_out')
                if data.get('AMOUNT') < 0:
                    payment_data['payment_type'] = 'inbound'
                    payment_method = self.env.ref('account.account_payment_method_manual_in')
            elif not data.get('IS_PAYMENT'):
                payment_data['payment_type'] = 'inbound'
                payment_data['partner_type'] = 'customer'
                payment_method = self.env.ref('account.account_payment_method_manual_in')
                if data.get('AMOUNT') < 0:
                    payment_data['payment_type'] = 'outbound'
                    payment_method = self.env.ref('account.account_payment_method_manual_out')
            if payment_method:
                payment_data['payment_method_id'] = payment_method.id
            # Partner
            if data.get('PARTNER_ID') and company:
                partner = self.env['res.partner'].sudo().search([('id', '=', data.get('PARTNER_ID')), ('company_id', '=', company.id)])
                if partner:
                    payment_data['partner_id'] = partner.id
                else:
                    return {'error_msg':'Invalid Partner with company'}
            else:
                return {'error_msg':'Partner not found'}
            #Payment Date
            if data.get('PAY_DATE'):
                payment_date = datetime.strptime(data.get('PAY_DATE'),"%Y-%m-%d").date()
                payment_data['payment_date'] = payment_date.strftime(DEFAULT_SERVER_DATE_FORMAT)
            if data.get('TRANS_REF'):
                payment_data['communication'] = data.get('TRANS_REF')
            # Payment Journal
            if data.get('JOURNAL_ID') and company:
                journal = self.env['account.journal'].sudo().search([('id', '=', data.get('JOURNAL_ID')), ('company_id', '=', company.id)])
                if journal:
                    payment_data['journal_id'] = journal.id
                else:
                    return {'error_msg': 'Invalid Journal with company'}
            else:
                return {'error_msg': 'Invalid JOURNAL_ID'}
            # Currency id
            if data.get('CURRENCY_ID'):
                payment_data['currency_id'] = data.get('CURRENCY_ID')
            else:
                return {'error_msg':'Invalid CURRENCY_ID'}
            if data.get('AMOUNT'):
                payment_data['amount'] = abs(data.get('AMOUNT', 0.0))
        except Exception as e:
            return {'error_msg':'%s:%s' %(data.get('PAY_TRANS_ID'), e)}

        return {'payment_data': payment_data}

    @api.multi
    def run_connector_payment(self):
        '''
           method to connect mssql and create payments
        '''
        for connector in self.sudo():
            connection = False

            if not connector.payment_model:
                if self._context.get('raise_error'):
                    raise UserError(_("Please check the Payment Model"))
                connector.register_log(model="Not Defined", msg="Please check the Payment Model")
                continue

            try:
                connection = pymssql.connect(connector.host, connector.username, connector.password, connector.db_name)
                cursor = connection.cursor(as_dict=True)
                select_query = 'SELECT TOP %s * FROM %s WHERE ODOO_IS_READ=0' %(connector.limit, connector.payment_model)
                cursor.execute(select_query)
                cursor_data = cursor.fetchall()
            except Exception as e:
                if self._context.get('raise_error'):
                    raise UserError(_("Connection Failed! Here is what we got instead:\n\n %s") % (e))
                else:
                    connector.register_log(model=connector.payment_model, msg="Connection Failed! Here is what we got instead:\n\n %s" % (e))
                    continue

            if cursor_data and not 'PAY_TRANS_ID' in cursor_data[0].keys():
                warning = "PAY_TRANS_ID not found in the model %s. Please check the Payment Model" %(connector.payment_model)
                if self._context.get('raise_error'):
                    raise UserError(_(warning))
                connector.register_log(model=connector.payment_model, msg=warning)
                continue

            company_data = {}
            for data in cursor_data:
                if data.get('COMPANY_ID') not in company_data.keys():
                    company_data[data.get('COMPANY_ID')] = [data]
                else:
                    company_data[data.get('COMPANY_ID')].append(data)

            for company_id, data_value in company_data.items():
                if connector.env.user.company_id.id != company_id:
                    connector.env.user.company_id = company_id
                for data in data_value:
                    payment_data = connector.get_payment_data(data)
                    payment_vals = payment_data.get('payment_data', False)
                    if payment_data.get('error_msg', False) and data.get('PAY_TRANS_ID'):
                        vals = (connector.payment_model, payment_data.get('error_msg'), data.get('PAY_TRANS_ID'))
                        update_query  = "UPDATE %s set ODOO_READ_SUCCESS=0, ODOO_ERROR_MESSAGE='%s' where PAY_TRANS_ID=%s" %vals
                        connector.execute_update_query(connection, cursor, update_query, data.get('PAY_TRANS_ID'), connector.payment_model)
                        connector.register_log(model=connector.payment_model, msg='PAY_TRANS_ID :%s \nMsg: %s \n\nData: %s' %(data.get('PAY_TRANS_ID'), payment_data.get('error_msg'), data))
                    elif payment_vals and data.get('PAY_TRANS_ID'):
                        try:
                            payment = self.env['account.payment'].sudo().create(payment_vals)
                            if data.get('CURRENCY_RATE'):
                                currency_rate = payment.sudo().currency_id.rate_ids.filtered(lambda rec: rec.company_id and \
                                            rec.company_id.id==payment.company_id.id and rec.name==payment.payment_date)
                                if currency_rate and currency_rate.rate != data.get('CURRENCY_RATE'):
                                    currency_rate.rate = data.get('CURRENCY_RATE')
                                elif not currency_rate:
                                    self.env['res.currency.rate'].sudo().create({
                                        'name': payment.payment_date,
                                        'currency_id': payment.currency_id.id,
                                        'company_id': payment.company_id.id,
                                        'rate': data.get('CURRENCY_RATE')
                                    })
                            payment.sudo().post()
                            date_time_now = fields.Datetime.now()
                            values = (connector.payment_model, date_time_now, payment.move_name, data.get('PAY_TRANS_ID'))
                            success_query = "UPDATE %s set ODOO_READ_SUCCESS=1, ODOO_IS_READ=1, ODOO_IS_READ_ON='%s', ODOO_JOURNAL_REF='%s',ODOO_ERROR_MESSAGE='' where PAY_TRANS_ID=%s;" %values
                            connector.execute_update_query(connection, cursor, success_query, data.get('PAY_TRANS_ID'), connector.payment_model)
                        except Exception as e:
                            logging.error(e)
                            msg = e and str(e).replace("'","")
                            vals = (connector.payment_model, msg, data.get('PAY_TRANS_ID'))
                            update_query  = "UPDATE %s set ODOO_READ_SUCCESS=0, ODOO_ERROR_MESSAGE='%s' where PAY_TRANS_ID=%s" %vals
                            connector.execute_update_query(connection, cursor, update_query, data.get('PAY_TRANS_ID'), connector.payment_model)
                            connector.register_log(model=connector.payment_model, msg='PAY_TRANS_ID :%s \nMsg: %s \n\n Data: %s' %(data.get('PAY_TRANS_ID'), msg, data))

            if connection:
                connection.close()
        return True

    @api.multi
    def run_mssql_connector_payment_cron(self):
        '''
            cron method to pull data for payments
        '''
        for connector in self.env['mssql.connector'].sudo().search([]):
            connector.sudo().run_connector_payment()
        return True

    def get_internal_payment_data(self, data):
        """
        Get the internal payment values

        :rtype           : `dict`
        :returns         : dictionary with keys payment_data or error_msg

        """
        if not data:
            return {'error_msg': 'Data not found !'}

        payment_data = {}
        company = False
        payment_method = False
        try:
            # Company
            if data.get('COMPANY_ID'):
                payment_data['company_id'] = data.get('COMPANY_ID')
                company = self.env['res.company'].sudo().browse([payment_data.get('company_id')])
                if not company:
                    return {'error_msg':'Invalid Company'}
            else:
                return {'error_msg':'Invalid Company'}

            # Payment Type
            payment_data['payment_type'] = 'transfer'
            payment_method = self.env.ref('account.account_payment_method_manual_in')
            if data.get('PAYMENT_AMOUNT') < 0:
                payment_method = self.env.ref('account.account_payment_method_manual_out')
            if payment_method:
                payment_data['payment_method_id'] = payment_method.id

            #Payment Date
            if data.get('PAY_DATE'):
                payment_date = datetime.strptime(data.get('PAY_DATE'),"%Y-%m-%d").date()
                payment_data['payment_date'] = payment_date.strftime(DEFAULT_SERVER_DATE_FORMAT)
            if data.get('MEMO'):
                payment_data['communication'] = data.get('MEMO')

            # Payment Journal
            if data.get('PAYMENT_JOURNAL_ID') and company:
                journal = self.env['account.journal'].sudo().search([('id', '=', data.get('PAYMENT_JOURNAL_ID')), ('company_id', '=', company.id)])
                if journal:
                    payment_data['journal_id'] = journal.id
                else:
                    return {'error_msg': 'Invalid Payment Journal with company'}
            else:
                return {'error_msg': 'Invalid PAYMENT_JOURNAL_ID'}

            # Recipient Journal
            if data.get('RECIPIENT_JOURNAL_ID') and company:

                journal = self.env['account.journal'].sudo().search([('id', '=', data.get('RECIPIENT_JOURNAL_ID')), ('company_id', '=', company.id)])
                if journal:
                    payment_data['destination_journal_id'] = journal.id
                    # if journal.currency_id and journal.currency_id.id == data.get('RECIPIENT_CURRENCY_ID'):
                    #     payment_data['destination_journal_id'] = journal.id
                    # else:
                    #     return {'error_msg': ' Recipient Journal with different RECIPIENT_CURRENCY_ID'}
                else:
                    return {'error_msg': 'Invalid Recipient Journal with company'}
            else:
                return {'error_msg': 'Invalid RECIPIENT_JOURNAL_ID'}

            # Payment Currency id
            if data.get('PAYMENT_CURRENCY_ID'):
                payment_data['currency_id'] = data.get('PAYMENT_CURRENCY_ID')
            else:
                return {'error_msg':'Invalid PAYMENT_CURRENCY_ID'}

            #Amount
            if data.get('PAYMENT_AMOUNT'):
                payment_data['amount'] = abs(data.get('PAYMENT_AMOUNT', 0.0))

        except Exception as e:
            return {'error_msg':'%s:%s' %(data.get('INTERNAL_PAY_TRANS_ID'), e)}

        return {'payment_data': payment_data}

    @api.multi
    def run_connector_internal_payment(self):
        '''
           method to connect mssql and create internal payments
        '''
        for connector in self.sudo():
            connection = False

            if not connector.internal_payment_model:
                if self._context.get('raise_error'):
                    raise UserError(_("Please check the Payment Model"))
                continue

            try:
                connection = pymssql.connect(connector.host, connector.username, connector.password, connector.db_name)
                cursor = connection.cursor(as_dict=True)
                select_query = 'SELECT TOP %s * FROM %s WHERE ODOO_IS_READ=0' %(connector.limit, connector.internal_payment_model)
                cursor.execute(select_query)
                cursor_data = cursor.fetchall()
            except Exception as e:
                if self._context.get('raise_error'):
                    raise UserError(_("Connection Failed! Here is what we got instead:\n\n %s") % (e))
                else:
                    connector.register_log(model=connector.internal_payment_model, msg="Connection Failed! Here is what we got instead:\n\n %s" % (e))
                    continue

            if cursor_data and not 'INTERNAL_PAY_TRANS_ID' in cursor_data[0].keys():
                warning = "INTERNAL_PAY_TRANS_ID not found in the model %s. Please check the Internal Payment Model" %(connector.internal_payment_model)
                if self._context.get('raise_error'):
                    raise UserError(_(warning))
                connector.register_log(msg=warning)
                continue

            company_data = {}
            for data in cursor_data:
                if data.get('COMPANY_ID') not in company_data.keys():
                    company_data[data.get('COMPANY_ID')] = [data]
                else:
                    company_data[data.get('COMPANY_ID')].append(data)

            for company_id, data_value in company_data.items():
                if connector.env.user.company_id.id != company_id:
                    connector.env.user.company_id = company_id
                for data in data_value:
                    payment_data = connector.get_internal_payment_data(data)

                    payment_vals = payment_data.get('payment_data', False)
                    if payment_data.get('error_msg', False) and data.get('INTERNAL_PAY_TRANS_ID'):
                        vals = (connector.internal_payment_model, payment_data.get('error_msg'), data.get('INTERNAL_PAY_TRANS_ID'))
                        update_query  = "UPDATE %s set ODOO_READ_SUCCESS=0, ODOO_ERROR_MESSAGE='%s' where INTERNAL_PAY_TRANS_ID=%s" %vals
                        connector.execute_update_query(connection, cursor, update_query, data.get('INTERNAL_PAY_TRANS_ID'), connector.internal_payment_model)
                        connector.register_log(model=connector.internal_payment_model, msg='INTERNAL_PAY_TRANS_ID :%s \nMsg: %s \n\nData: %s' %(data.get('INTERNAL_PAY_TRANS_ID'), payment_data.get('error_msg'), data))
                    elif payment_vals and data.get('INTERNAL_PAY_TRANS_ID'):
                        try:
                            payment = self.env['account.payment'].sudo().create(payment_vals)
                            if data.get('RECIPIENT_CURRENCY_RATE'):
                                currency_rate = self.env['res.currency.rate'].sudo().search([('currency_id', '=', data.get('RECIPIENT_CURRENCY_ID')), ('company_id', '=', payment.company_id.id), ('name', '=', payment.payment_date)])
                                logging.error((data.get('RECIPIENT_CURRENCY_ID'),payment.company_id.id,payment.payment_date,currency_rate))
                                if currency_rate and currency_rate.rate != data.get('RECIPIENT_CURRENCY_RATE'):
                                    currency_rate.rate = data.get('RECIPIENT_CURRENCY_RATE')
                                elif not currency_rate:
                                    currency_rate_id = self.env['res.currency.rate'].sudo().create({
                                        'name': payment.payment_date,
                                        'currency_id': data.get('RECIPIENT_CURRENCY_ID'),
                                        'company_id': payment.company_id.id,
                                        'rate': data.get('RECIPIENT_CURRENCY_RATE')
                                    })
                            payment.sudo().post()
                            date_time_now = fields.Datetime.now()

                            recipient_journal_name = ''
                            recipient_journal = payment.move_line_ids.filtered(lambda r: r.full_reconcile_id and r.move_id.name != payment.move_name)
                            if recipient_journal:
                                recipient_journal_name = recipient_journal[0].move_id.name

                            values = (connector.internal_payment_model, date_time_now, payment.move_name, recipient_journal_name, data.get('INTERNAL_PAY_TRANS_ID'))

                            success_query = "UPDATE %s set ODOO_READ_SUCCESS=1, ODOO_IS_READ=1, ODOO_IS_READ_ON='%s', ODOO_PAYMENT_JOURNAL_REF='%s', ODOO_RECIPIENT_JOURNAL_REF='%s', ODOO_ERROR_MESSAGE='' where INTERNAL_PAY_TRANS_ID=%s;" %values
                            connector.execute_update_query(connection, cursor, success_query, data.get('INTERNAL_PAY_TRANS_ID'), connector.internal_payment_model)


                        except Exception as e:
                            logging.error(e)
                            msg = e and str(e).replace("'","")
                            vals = (connector.internal_payment_model, msg, data.get('INTERNAL_PAY_TRANS_ID'))
                            update_query  = "UPDATE %s set ODOO_READ_SUCCESS=0, ODOO_ERROR_MESSAGE='%s' where INTERNAL_PAY_TRANS_ID=%s" %vals
                            connector.execute_update_query(connection, cursor, update_query, data.get('INTERNAL_PAY_TRANS_ID'), connector.internal_payment_model)
                            connector.register_log(model=connector.internal_payment_model, msg='INTERNAL_PAY_TRANS_ID :%s \nMsg: %s \n\n Data: %s' %(data.get('INTERNAL_PAY_TRANS_ID'), msg, data))

            if connection:
                connection.close()
        return True

    @api.multi
    def run_mssql_connector_internal_payment_cron(self):
        '''
            cron method to pull data for internal payments
        '''
        for connector in self.env['mssql.connector'].sudo().search([]):
            connector.sudo().run_connector_internal_payment()
        return True


MSSQLConnector()
