# -*- coding: utf-8 -*-
from .baseapi import BaseAPI


class Balance(BaseAPI):
    def __init__(self, *args, **kwargs):
        self.month_to_date_balance = None
        self.account_balance = None
        self.month_to_date_usage = None
        self.generated_at = None

        super(Balance, self).__init__(*args, **kwargs)

    @classmethod
    def get_object(cls, requester):
        """
            Class method that will return an Balance object.
        """
        acct = cls(requester=requester)
        acct.load()
        return acct

    def load(self):
        # URL https://api.digitalocean.com/customers/my/balance
        balance = self.get_data("customers/my/balance")

        for attr in balance.keys():
            setattr(self, attr, balance[attr])

    def __str__(self):
        return "<Balance: %s>" % (self.account_balance)
