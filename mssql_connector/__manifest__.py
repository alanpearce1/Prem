# -*- coding: utf-8 -*-

{
    'name': 'MSSQL Connector',
    'version': '11.0',   
    'category': 'Accounting & Finance',
    'summary': "Accounting",
    'description': """
ODOO Integration with MSSQL
                              
       """,
    'author': 'Confianz Global,Inc.',
    'website': 'https://www.confianzit.com',
    'images': [],
    'data': [
             'security/ir.model.access.csv',
             'data/data.xml',
             'views/connector_view.xml',],
#             'views/account_invoice_view.xml'],
    'depends':  ['account_accountant'],
    'installable': True,
    'auto_install': False,
    'application': False,
}































# vim:expandtab:smartindent:tabstop=4:softtabstop=4:shiftwidth=4:
