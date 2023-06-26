.PHONY : contrib all

all: 
		make -C contrib all
	
contrib : 
		make -C contrib

clean: 
		make -C contrib clean


