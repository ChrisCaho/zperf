PREFIX ?= /usr/local
BINDIR ?= $(PREFIX)/bin

.PHONY: install uninstall

install:
	install -m 755 zperf.py $(DESTDIR)$(BINDIR)/zperf

uninstall:
	rm -f $(DESTDIR)$(BINDIR)/zperf
