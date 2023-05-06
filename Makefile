$(eval export PYTHONPATH=$(shell pwd))

do_pytest:
	@if ! (pip -q show pytest); then\
        pip install pytest;\
    fi
	@pytest -v