from __future__ import annotations

from bithumb.client import BithumbRESTClient
from bithumb.credentials import load_credentials

from meatna.core.balance import QuoteBalances


class AccountService:

    def __init__(self) -> None:
        creds = load_credentials()
        self._client = BithumbRESTClient(access_key=creds.access_key, secret_key=creds.secret_key)

    async def __aenter__(self) -> "AccountService":
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    async def fetch_balances(self) -> QuoteBalances:
        accounts = await self._client.private.get_accounts()
        balance_map = {acct.currency.upper(): acct for acct in accounts}
        krw = self._available(balance_map.get("KRW"))
        btc = self._available(balance_map.get("BTC"))
        usdt = self._available(balance_map.get("USDT"))
        return QuoteBalances(krw=krw, btc=btc, usdt=usdt)

    @staticmethod
    def _available(account) -> float:
        if account is None:
            return 0.0
        return max(float(account.balance) - float(account.locked), 0.0)
