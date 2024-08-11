-- Table: public.maincode

-- DROP TABLE IF EXISTS public.maincode;

CREATE TABLE IF NOT EXISTS public.maincode
(
    code character varying(50) COLLATE pg_catalog."default" NOT NULL,
    cname character varying(50) COLLATE pg_catalog."default" NOT NULL,
    listed character varying(50) COLLATE pg_catalog."default" NOT NULL,
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.maincode
    OWNER to mini;

GRANT ALL ON TABLE public.maincode TO mini;