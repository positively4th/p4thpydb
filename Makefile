.PHONY : requirements contrib all

all: 
		make requirements
		make -C contrib all
	
requirements : 
	python -m venv .venv \
	&& source .venv/bin/activate \
	&& pip install -r requirements.txt

contrib : 
		make -C contrib

clean: 
		rm -rf .venv \
		&& 1make -C contrib clean


