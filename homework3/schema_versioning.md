Schema versioning is the practice of tracking and controlling changes to a
database/data structure over time. Here, it means evolving the message models in
messages.py, which are the contract between the exchange and each client, 
without forcing the exchange and its clients to upgrade at the same moment. This
is necessary because the exchange and clients may get updated at different
times. The codebase does this by only ever adding fields with defaults. To add a
field without breaking older clients, the field must have a default, ideally
None, so that messages without it still validate and "not sent" can't be
confused with a real value. A new client can then still validate messages from
an older exchange that never sends it, and older clients receiving messages 
from a new exchange are unaffected, because pydantic ignores undeclared
fields by default. It is also important not to change an existing field's name,
type, or meaning, or tighten a constraint the sender might violate. Isinstance
checks on dicts only check that the frame is a dict, not which message it is
or whether it is well formed. The discriminated union reads the type literal and
chooses the model to validate it against, then validates every field's presence,
type, and constraints using a single TypeAdapter.validate_json call. This means
that malformed or unknown messages fail there, specifying the error with a
ValidationError, rather than surfacing later as a KeyError or a bug in the 
trading logic. Additionally, what comes out is a typed object rather than a
dict that every consumer must check by hand.