---
sidebar_position: 4
---

# Creating accounts and transfers in the Node CLI

Once you've got the TigerBeetle server running, let's connect to the
running server and do some accounting!


First let's create two accounts. (Don't worry about the details, you
can read about them later.)

```bash
$ tigerbeetle client --addresses=3000 create-accounts \
    "id:1 ledger:1 code:718" \
	"id:2 ledger:1 code:718"
info(message_bus): connected to replica 0
```

Now create a transfer of `10` (of some amount/currency) between the two accounts.

```javascript
$ tigerbeetle client --addresses=3000 create-transfers \
    "id:1 debit_account_id:1 credit_account_id:2 ledger:1 code:718 amount:10"
info(message_bus): connected to replica 0
```

Now, the amount of `10` has been credited to account `2` and debited
from account `1`. Let's query TigerBeetle for these two accounts to
verify!

```javascript
$ tigerbeetle client --addresses=1025 lookup-accounts id:1 id:2
info(message_bus): connected to replica 0
{
  "id":              "1",
  "user_data":       "0",
  "ledger":          "1",
  "code":            "718",
  "flags":           "",
  "debits_pending":  "0",
  "debits_posted":   "10",
  "credits_pending": "0",
  "credits_posted":  "0"
}
{
  "id":              "2",
  "user_data":       "0",
  "ledger":          "1",
  "code":            "718",
  "flags":           "",
  "debits_pending":  "0",
  "debits_posted":   "0",
  "credits_pending": "0",
  "credits_posted":  "10"
}
```

And indeed you can see that account `1` has `debits_posted` as `10`
and account `2` has `credits_posted` as `10`. The `10` amount is fully
accounted for!
