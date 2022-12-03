import sentry
import pendulum as plm
import random

class ReliableAsset(sentry.Asset):
    def __init__(self, key, dependencies):
        self._built = False
        super(ReliableAsset,self).__init__(key,dependencies)

    def build(self):
        self._built = True
        self._ts = plm.now()
        return 3

    def timestamp(self):
        return self._ts if self._built else sentry.AssetStatus.Unavailable

class UnreliableAsset(sentry.Asset):
    def __init__(self, key, dependencies):
        self._built = False
        super(UnreliableAsset,self).__init__(key,dependencies)

    def build(self):
        # with P = 0.4, the asset is created successfully
        # with P = 0.3, the asset build fails before creation
        # with P = 0.3, the asset build fails after creation
        r = random.random()
        if r <= 0.4:
            self._built = True
            self._ts = plm.now()
        elif r <= 0.7:
            raise Exception
        else:
            self._built = True
            self._ts = plm.now()
            raise Exception
        return 3

    def timestamp(self):
        return self._ts if self._built else sentry.AssetStatus.Unavailable

# dependencies: Reliable -> Unreliable -> Reliable

g = sentry.AssetGraph()

a = ReliableAsset("Reliable", [])
li = []
final = []
for i in range(20000):
    a1 = ReliableAsset(f"Reliable1{i}", [a])
    a2 = UnreliableAsset(f"Unreliable{i}", [a1])
    a3 = ReliableAsset(f"Reliable2{i}", [a2])
    li.extend([a1,a2,a3])
    final.append(a3)
g.add_assets(li)
b = ReliableAsset("Final", final)
g.add_assets([b])

#g.visualize()

g.propagate_status()
g.build()


g.visualize()

