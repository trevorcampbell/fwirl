import momo


#momo.summarize("test_graph")

momo.ls("test_graph", True, False)

momo.pause("test_graph", asset="blah")
momo.pause("test_graph", schedule="blah")
momo.pause("test_graph")
momo.pause("test_graph", schedule="blah", asset="bloh")

momo.unpause("test_graph", asset="blah")
momo.unpause("test_graph", schedule="blah")
momo.unpause("test_graph")
momo.unpause("test_graph", schedule="blah", asset="bloh")


momo.schedule("test_graph", "blah", "* * * * *")
momo.unschedule("test_graph", "blah")


momo.build("test_graph")
momo.refresh("test_graph")
