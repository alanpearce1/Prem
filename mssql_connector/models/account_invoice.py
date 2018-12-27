# -*- coding: utf-8 -*-

from odoo import fields, models, api, _

class AccountInvoice(models.Model):

    _inherit = 'account.invoice'

    x_studio_clinicid = fields.Integer(string='x_studio_clinicid')
    x_studio_clinicid_1 = fields.Integer(string='x_studio_clinicid_1')
    x_studio_soid_2 = fields.Char(string='x_studio_soid_2')
    x_studio_soid_3 = fields.Char(string='x_studio_soid_3')
    x_studio_custid = fields.Integer(string='x_studio_custid')
    x_studio_custid_1 = fields.Integer(string='x_studio_custid_1')

AccountInvoice()

class AccountMove(models.Model):
    _inherit = "account.move"

    @api.model
    def create(self, vals):
        move = super(AccountMove, self).create(vals)
        if self._context.get('TRANS_REF'):
            move.name = self._context.get('TRANS_REF')
        return move

AccountMove()
        
