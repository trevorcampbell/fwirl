import fwirl
import pendulum as plm
import random
import json


class ReliableAsset(fwirl.Asset):
    def __init__(self, key, dependencies, resources = None, group = None, subgroup = None):
        self._built = False
        super(ReliableAsset,self).__init__(key, dependencies, resources, group, subgroup)

    def build(self):
        self._built = True
        self._ts = plm.now()
        return 3

    def timestamp(self):
        return self._ts if self._built else fwirl.AssetStatus.Unavailable

class UnreliableAsset(fwirl.Asset):
    def __init__(self, key, dependencies, resources = None, group = None, subgroup = None):
        self._built = False
        super(UnreliableAsset,self).__init__(key, dependencies, resources, group, subgroup)

    def build(self):
        # with P = 0.4, the asset is created successfully
        # with P = 0.3, the asset build fails before creation
        # with P = 0.3, the asset build fails after creation
        r = random.random()
        if r > 0.01:
            self._built = True
            self._ts = plm.now()
        #elif r <= 0.7:
        #    raise Exception
        else:
            #self._built = True
            #self._ts = plm.now()
            raise Exception
        return 3

    def timestamp(self):
        return self._ts if self._built else fwirl.AssetStatus.Unavailable


with open('info.json', 'r') as f:
    info = json.load(f)
    notifiers = {
        "slack" : {
            "params": {
                "webhook_url": info["slack_webhook_url"]
            },
            "level" : "ERROR"
        },
        "email": {
            "params": {
                "subject": info["email"]["subject"],
                "from": info["email"]["from"],
                "to": info["email"]["to"],
                "password" : info["email"]["password"],
                "username" : info["email"]["username"],
                "host" : info["email"]["host"],
                "port" : info["email"]["port"],
                "tls" : info["email"]["tls"],
                "ssl" : info["email"]["ssl"]
            },
            "level" : "ERROR"
        }
    }

g = fwirl.AssetGraph("test_graph", notifiers=notifiers)

a = ReliableAsset("Reliable", [])
li = []
final = []
for i in range(200):
    a1 = ReliableAsset(f"Reliable1{i}", [a], group = 0, subgroup = i)
    a2 = UnreliableAsset(f"Unreliable{i}", [a1], group = 0, subgroup = i)
    a3 = ReliableAsset(f"Reliable2{i}", [a2], group = 0, subgroup = i)
    li.extend([a1,a2,a3])
    final.append(a3)
g.add_assets(li)
b = ReliableAsset("Final", final)
g.add_assets([b])

g.build()


