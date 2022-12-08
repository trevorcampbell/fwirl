import sentry


#sentry.summarize("test_graph")

sentry.ls("test_graph", True, False)

sentry.pause("test_graph", asset="blah")
sentry.pause("test_graph", schedule="blah")
sentry.pause("test_graph")
sentry.pause("test_graph", schedule="blah", asset="bloh")

sentry.unpause("test_graph", asset="blah")
sentry.unpause("test_graph", schedule="blah")
sentry.unpause("test_graph")
sentry.unpause("test_graph", schedule="blah", asset="bloh")


sentry.schedule("test_graph", "blah", "* * * * *")
sentry.unschedule("test_graph", "blah")


sentry.build("test_graph")
sentry.refresh("test_graph")
