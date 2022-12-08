import fwirl


#fwirl.summarize("test_graph")

fwirl.ls("test_graph", True, False)

fwirl.pause("test_graph", asset="blah")
fwirl.pause("test_graph", schedule="blah")
fwirl.pause("test_graph")
fwirl.pause("test_graph", schedule="blah", asset="bloh")

fwirl.unpause("test_graph", asset="blah")
fwirl.unpause("test_graph", schedule="blah")
fwirl.unpause("test_graph")
fwirl.unpause("test_graph", schedule="blah", asset="bloh")


fwirl.schedule("test_graph", "blah", "* * * * *")
fwirl.unschedule("test_graph", "blah")


fwirl.build("test_graph")
fwirl.refresh("test_graph")
