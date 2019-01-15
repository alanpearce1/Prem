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
    model_name = fields.Char(string='Model', required=True)
    db_name = fields.Char(string='Database', required=True)
    limit = fields.Integer(default=500, string='Limit', required=True)
    connector_type = fields.Selection([
        ('invoice', 'Invoice Creation'),
        ('payment', 'Payment Creation')
    ], string='Type', required=True)


    @api.multi
    def name_get(self):
        res = []
        for connector in self:
            name = '%s - %s' % (connector.db_name, connector.model_name)
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
    def register_log(self, msg=False):
        '''
           method to log error on query execution
        '''
        self.ensure_one()
        if msg:
            self.env['mssql.connector.log'].create({
                                        'connector_id':self.id,
                                        'db_name':self.db_name,
                                        'model_name':self.model_name,
                                        'log':msg,
                                    })
        return True

    @api.model
    def execute_update_query(self, conn, cursor, query, trans_id):
        '''
           method for update query execution
        '''
        try:
            if conn and cursor and query:
                cursor.execute(query)
                conn.commit()
        except Exception as e:
            self.register_log(msg='TRANS_ID :%s,  Msg: %s, Query: %s' %(trans_id, e, query))

    @api.multi
    def run_connector(self):
        self.ensure_one()
        if hasattr(self, 'run_connector_%s' % self.connector_type):
            return getattr(self, 'run_connector_%s' % self.connector_type)()
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
            try:
                connection = pymssql.connect(connector.host, connector.username, connector.password, connector.db_name)
                cursor = connection.cursor(as_dict=True)
                select_query = 'SELECT TOP %s * FROM %s WHERE ODOO_IS_READ=0' %(connector.limit, connector.model_name)
                cursor.execute(select_query)
                cursor_data = cursor.fetchall()
            except Exception as e:
                if self._context.get('raise_error'):
                    raise UserError(_("Connection Test Failed! Here is what we got instead:\n\n %s") % (e))
                else:
                    connector.register_log(msg="Connection Test Failed! Here is what we got instead:\n\n %s" % (e))
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
                            vals = (connector.model_name, date_time_now, data.get('TRANS_ID'))
                            update_query = "UPDATE %s set ODOO_IS_READ=1, ODOO_IS_READ_ON='%s' where TRANS_ID=%s" %vals
                            connector.execute_update_query(connection, cursor, update_query, data.get('TRANS_ID'))
                        except Exception as e:
                            msg = e and str(e).replace("'","")
                            vals = (connector.model_name, msg, data.get('TRANS_ID'))
                            update_query  = "UPDATE %s set ODOO_READ_SUCCESS=0, ODOO_ERROR_MESSAGE='%s' where TRANS_ID=%s" %vals
                            connector.execute_update_query(connection, cursor, update_query, data.get('TRANS_ID'))
                            connector.register_log(msg='TRANS_ID :%s \nMsg: %s \n\nData: %s' %(data.get('TRANS_ID'), msg, data))
                        continue
                    invoice_data = connector.get_invoice_data(data)
                    invoice_vals = invoice_data.get('invoice_data', False)
                    if invoice_data.get('error_msg', False) and data.get('TRANS_ID'):
                        vals = (connector.model_name, invoice_data.get('error_msg'), data.get('TRANS_ID'))
                        update_query  = "UPDATE %s set ODOO_READ_SUCCESS=0, ODOO_ERROR_MESSAGE='%s' where TRANS_ID=%s" %vals
                        connector.execute_update_query(connection, cursor, update_query, data.get('TRANS_ID'))
                        connector.register_log(msg='TRANS_ID :%s \nMsg: %s \n\nData: %s' %(data.get('TRANS_ID'), invoice_data.get('error_msg'), data))
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
                                vals = (connector.model_name, 'Duplicate TRANS_REF', data.get('TRANS_ID'))
                                update_query  = "UPDATE %s set ODOO_READ_SUCCESS=0, ODOO_ERROR_MESSAGE='%s' where TRANS_ID=%s" %vals
                                connector.execute_update_query(connection, cursor, update_query, data.get('TRANS_ID'))
                                connector.register_log(msg='TRANS_ID :%s \nMsg: Duplicate TRANS_REF \n\n Data: %s' %(data.get('TRANS_ID'), data))
                                invoice.action_cancel()
                            else:
                                invoice.with_context({'TRANS_REF':data.get('TRANS_REF')}).sudo().action_invoice_open()
                                date_time_now = fields.Datetime.now()
                                values = (connector.model_name, date_time_now, invoice.move_id.name, data.get('TRANS_ID'))
                                success_query = "UPDATE %s set ODOO_READ_SUCCESS=1, ODOO_IS_READ=1, ODOO_IS_READ_ON='%s', ODOO_JOURNAL_REF='%s',ODOO_ERROR_MESSAGE='' where TRANS_ID=%s;" %values
                                connector.execute_update_query(connection, cursor, success_query, data.get('TRANS_ID'))
                        except Exception as e:
                            logging.error(e)
                            msg = e and str(e).replace("'","")
                            vals = (connector.model_name, msg, data.get('TRANS_ID'))
                            update_query  = "UPDATE %s set ODOO_READ_SUCCESS=0, ODOO_ERROR_MESSAGE='%s' where TRANS_ID=%s" %vals
                            connector.execute_update_query(connection, cursor, update_query, data.get('TRANS_ID'))
                            connector.register_log(msg='TRANS_ID :%s \nMsg: %s \n\n Data: %s' %(data.get('TRANS_ID'), msg, data))
            if connection:
                connection.close()
        return True

    @api.multi
    def run_mssql_connector_cron(self):
        '''
            cron method to pull data
        '''
        for connector in self.env['mssql.connector'].sudo().search([('connector_type', '=', 'invoice')]):
            connector.sudo().run_connector()
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
            try:
                connection = pymssql.connect(connector.host, connector.username, connector.password, connector.db_name)
                cursor = connection.cursor(as_dict=True)
                select_query = 'SELECT TOP %s * FROM %s WHERE ODOO_IS_READ=0' %(connector.limit, connector.model_name)
                cursor.execute(select_query)
                cursor_data = cursor.fetchall()
            except Exception as e:
                if self._context.get('raise_error'):
                    raise UserError(_("Connection Test Failed! Here is what we got instead:\n\n %s") % (e))
                else:
                    connector.register_log(msg="Connection Test Failed! Here is what we got instead:\n\n %s" % (e))
                    continue

            if connector.connector_type == 'payment':
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
                            vals = (connector.model_name, payment_data.get('error_msg'), data.get('PAY_TRANS_ID'))
                            update_query  = "UPDATE %s set ODOO_READ_SUCCESS=0, ODOO_ERROR_MESSAGE='%s' where PAY_TRANS_ID=%s" %vals
                            connector.execute_update_query(connection, cursor, update_query, data.get('PAY_TRANS_ID'))
                            connector.register_log(msg='PAY_TRANS_ID :%s \nMsg: %s \n\nData: %s' %(data.get('PAY_TRANS_ID'), payment_data.get('error_msg'), data))
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
                                values = (connector.model_name, date_time_now, payment.move_name, data.get('PAY_TRANS_ID'))
                                success_query = "UPDATE %s set ODOO_READ_SUCCESS=1, ODOO_IS_READ=1, ODOO_IS_READ_ON='%s', ODOO_JOURNAL_REF='%s',ODOO_ERROR_MESSAGE='' where PAY_TRANS_ID=%s;" %values
                                connector.execute_update_query(connection, cursor, success_query, data.get('PAY_TRANS_ID'))
                            except Exception as e:
                                logging.error(e)
                                msg = e and str(e).replace("'","")
                                vals = (connector.model_name, msg, data.get('PAY_TRANS_ID'))
                                update_query  = "UPDATE %s set ODOO_READ_SUCCESS=0, ODOO_ERROR_MESSAGE='%s' where PAY_TRANS_ID=%s" %vals
                                connector.execute_update_query(connection, cursor, update_query, data.get('PAY_TRANS_ID'))
                                connector.register_log(msg='PAY_TRANS_ID :%s \nMsg: %s \n\n Data: %s' %(data.get('PAY_TRANS_ID'), msg, data))

            if connection:
                connection.close()
        return True

    @api.multi
    def run_mssql_connector_payment_cron(self):
        '''
            cron method to pull data for payments
        '''
        for connector in self.env['mssql.connector'].sudo().search([('connector_type', '=', 'payment')]):
            connector.sudo().run_connector()
        return True


MSSQLConnector()
