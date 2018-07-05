LINUXGOPATH   = $(shell cd $${GOPATH} ;pwd)
# In windows git bash, turn GOPATH to linux path style
BIN           = $(GOPATH)/bin
ON            = $(BIN)/on
GO_BINDATA    = $(BIN)/go-bindata
NODE_BIN      = $(shell npm bin)
PID           = .pid
GO_FILES      = $(filter-out server/server/bindata.go, $(shell find ./server  -type f -name "*.go"))
TEMPLATES     = $(wildcard server/data/templates/*.html)
BINDATA       = server/server/bindata.go
BINDATA_FLAGS = -pkg=server -prefix=client/dist -nomemcopy
IMPORT_PATH   = $(shell pwd | sed "s|^$(LINUXGOPATH)/src/||g")
STATICS       = client/dist
APP_NAME      = $(shell pwd | sed 's:.*/::')
TARGET        = $(APP_NAME)
GIT_HASH      = $(shell git rev-parse HEAD)
LDFLAGS       = -w -X main.commitHash=$(GIT_HASH)

build: $(ON) $(GO_BINDATA) clean $(TARGET)

clean:
	@rm -rf client/dist
	@rm -rf $(BINDATA)

$(ON):
	go install $(IMPORT_PATH)/vendor/github.com/olebedev/on

$(GO_BINDATA):
	go install $(IMPORT_PATH)/vendor/github.com/jteeuwen/go-bindata/...

$(STATICS):
	@cd client && npm run build

$(TARGET): $(BINDATA)
	go build -ldflags '$(LDFLAGS)' -o $@ $(IMPORT_PATH)/server

kill:
	@kill `cat $(PID)` || true

serve: $(ON) $(GO_BINDATA) clean $(BUNDLE) restart
	@BABEL_ENV=dev node hot.proxy &
	@$(NODE_BIN)/webpack --watch &
	@$(ON) -m 2 $(GO_FILES) $(TEMPLATES) | xargs -n1 -I{} make restart || make kill

restart: BINDATA_FLAGS += -debug
restart: LDFLAGS += -X main.debug=true
restart: $(BINDATA) kill $(TARGET)
	@echo restart the app...
	@$(TARGET) run & echo $$! > $(PID)

$(BINDATA): $(STATICS) $(GO_BINDATA)
	$(GO_BINDATA) $(BINDATA_FLAGS) -o=$@ client/dist/...

lint:
	@cd client && npm run lint || true
	@golint server/... || true

install: install-client install-server

install-client:
	@cd client && yarn install

install-server:
	@dep ensure -v
	@dep ensure -v --add github.com/olebedev/on
	@dep ensure -v --add github.com/jteeuwen/go-bindata/
