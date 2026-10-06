TOPIC := order.events
KAFKA := docker compose exec -T kafka /opt/kafka/bin
PSQL  := docker compose exec -T postgres psql -U outbox -d outbox -v ON_ERROR_STOP=1 -qtA

.DEFAULT_GOAL := help
.PHONY: help up down reset psql order outbox consume race

help:            ## list targets
	@grep -hE '^[a-z-]+:.*?##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/' | expand -t18

up:              ## start containers, apply schema, create topic
	docker compose up -d --wait
	@$(PSQL) < schema.sql
	@$(KAFKA)/kafka-topics.sh --bootstrap-server localhost:19092 --create --if-not-exists \
	  --topic $(TOPIC) --partitions 3 --replication-factor 1

down:            ## stop, keep data
	docker compose down

reset:           ## stop, wipe data
	docker compose down -v

psql:            ## shell on postgres
	docker compose exec postgres psql -U outbox -d outbox

order:           ## make order N=5 -- N orders, each with its outbox row in ONE statement
	@for i in $$(seq 1 $(or $(N),1)); do $(PSQL) -c "\
	  WITH o AS (INSERT INTO orders (id, customer_id, amount_cents) \
	             VALUES (gen_random_uuid(), 'cust-'||$$i, 1000) RETURNING *) \
	  INSERT INTO outbox (event_id, aggregate_id, event_type, payload) \
	  SELECT gen_random_uuid(), o.id, 'order.created', to_jsonb(o) FROM o"; done
	@echo "inserted $(or $(N),1) order(s)"

outbox:          ## rows by state
	@$(PSQL) -c "SELECT id, aggregate_id, published_at FROM outbox ORDER BY id"

consume:         ## tail the topic with key + headers
	$(KAFKA)/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic $(TOPIC) --from-beginning \
	  --formatter-property print.key=true --formatter-property print.headers=true

CONNECT := http://localhost:8083
.PHONY: connector connector-status connector-rm

connector-status: ## state of the connector and its task
	@curl -sf $(CONNECT)/connectors/debezium-connector/status | jq -c '{connector: .connector.state, tasks: [.tasks[].state]}'

connector-rm:    ## delete the connector
	@curl -sf -XDELETE $(CONNECT)/connectors/debezium-connector && echo "debezium-connector deleted"

connector:       ## register the debezium connector (needs the schema: make up first)
	@jq .config connector.json | curl -sf -XPUT $(CONNECT)/connectors/debezium-connector/config \
	  -H 'content-type: application/json' --data-binary @- > /dev/null && echo "debezium-connector registered"

race:            ## tx A takes the lower id but commits after tx B. both A and B reach the topic (WAL is in commit order)
	@out=$$(mktemp); \
	  $(KAFKA)/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic $(TOPIC) \
	    --formatter-property print.key=true --timeout-ms 12000 > $$out 2>/dev/null & \
	  sleep 3; \
	  ( printf "BEGIN;\nINSERT INTO outbox (event_id, aggregate_id, event_type, payload) VALUES (gen_random_uuid(),'A','order.created','{}');\n\\\\! sleep 4\nCOMMIT;\n" | $(PSQL) ) & \
	  sleep 0.5; \
	  printf "INSERT INTO outbox (event_id, aggregate_id, event_type, payload) VALUES (gen_random_uuid(),'B','order.created','{}');\n" | $(PSQL); \
	  wait; \
	  echo "--- outbox (last 2 rows)"; $(PSQL) -c "SELECT id, aggregate_id, published_at FROM outbox ORDER BY id DESC LIMIT 2"; \
	  echo "--- published during the race: both A and B reach the topic (WAL is in commit order)"; cat $$out; rm $$out
