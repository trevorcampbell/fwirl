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

a1 = ReliableAsset("Reliable1", [])
a2 = UnreliableAsset("Unreliable", [a1])
a3 = ReliableAsset("Reliable2", [a2])

g = sentry.AssetGraph()
g.add_assets([a1, a2, a3])

g.visualize()

g.propagate_status()
g.build()


g.visualize()

