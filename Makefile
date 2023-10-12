help: ## This help dialog
help h:
	@IFS=$$'\n' ; \
	help_lines=(`fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##/:/'`); \
	printf "%-30s %s\n" "target" "help" ; \
	printf "%-30s %s\n" "------" "----" ; \
	for help_line in $${help_lines[@]}; do \
		IFS=$$':' ; \
		help_split=($$help_line) ; \
		help_command=`echo $${help_split[0]} | sed -e 's/^ *//' -e 's/ *$$//'` ; \
		help_info=`echo $${help_split[2]} | sed -e 's/^ *//' -e 's/ *$$//'` ; \
		printf '\033[36m'; \
		printf "%-30s %s" $$help_command ; \
		printf '\033[0m'; \
		printf "%s\n" $$help_info; \
	done


#====================#
#== QUALITY CHECKS ==#
#====================#

lint: ## Run all enabled linters
	@echo "====================="
	@echo "-- Running linters --"
	@echo "====================="
	go list -f '{{.Dir}}/...' -m | xargs golangci-lint run --fix

test: ## Run all unit tests
	@echo "======================"
	@echo "- Running unit tests -"
	@echo "======================"
	go list -f '{{.Dir}}/...' -m | xargs go test --tags=unit -v -timeout 10s -count=1 -cover
