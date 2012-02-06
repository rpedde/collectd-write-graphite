pkglibdir=$(libdir)/collectd
pkglib_LTLIBRARIES = graphite.la
graphite_la_SOURCES = graphite.c
graphite_la_LDFLAGS = -module -avoid-version -shared $(LIBCARBON_LIBS)
graphite_la_CFLAGS = $(COLLECTD_CFLAGS) $(LIBCARBON_CFLAGS)