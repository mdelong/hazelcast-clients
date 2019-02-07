import hazelcast, logging
from hazelcast.serialization.api import Portable

FACTORY_ID=1
class Customer(Portable):
    CLASS_ID=1
    def __init__(self, id=None, name=None, mobile=None):
        self.id = id
        self.name = name
        self.mobile = mobile

    def write_portable(self, writer):
        writer.write_int("id", self.id)
        writer.write_utf("name", self.name)
        writer.write_utf("mobile", self.mobile)

    def get_factory_id(self):
        return FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID

def get_async(f):
    print("my_map.get_async: ", f.result())

def item_added(event):
    print("item added: ", event)

def item_removed(event):
    print("item removed: ", event)

def item_updated(event):
    print("item updated: ", event)

config = hazelcast.ClientConfig()
config.network_config.addresses.append('192.168.1.144')

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

client = hazelcast.HazelcastClient(config)
my_map = client.get_map("testmap").blocking()
my_map.add_entry_listener(include_value=True, added_func=item_added, removed_func=item_removed, updated_func=item_updated)

my_map.put("key1", "testvalue1")
future = my_map.get("key1")
#future.add_done_callback(get_async)
future = my_map.get("key2")
#future.add_done_callback(get_async)

my_map.put("key2", "testvalue2")
future = my_map.get("key1")
#future.add_done_callback(get_async)
future = my_map.get("key2")
#future.add_done_callback(get_async)

my_map.put("key1", "testvalue1replacement")
future = my_map.get("key1")
#future.add_done_callback(get_async)
future = my_map.get("key2")
#future.add_done_callback(get_async)

my_map.remove("key2")
future = my_map.get("key1")
#future.add_done_callback(get_async)
future = my_map.get("key2")
#future.add_done_callback(get_async)

client.shutdown()

"""config.serialization_config.portable_factories[FACTORY_ID] = {Customer.CLASS_ID: Customer}
client = hazelcast.HazelcastClient(config)
my_map = client.get_map("testobjectmap").blocking()
#my_map.add_entry_listener(include_value=True, added_func=item_added, removed_func=item_removed, updated_func=item_updated)

c1 = Customer(12345, "Michael John Delong", mobile="18007778888")
c2 = Customer(67890, "George Washington", mobile="180066699999")

my_map.put(str(c1.id), c1)
my_map.put(str(c2.id), c2)

sequence = my_map.values(sql("name ILIKE 'Michael%'"))
sequence = my_map.values()
for customer in sequence:
    print(customer.name)

client.shutdown()"""
