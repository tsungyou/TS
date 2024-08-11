-- Table: public.block_code3_deatil

-- DROP TABLE IF EXISTS public.block_code3_deatil;

CREATE TABLE IF NOT EXISTS public.block_code3_deatil
(
    code character varying(50) COLLATE pg_catalog."default",
    da timestamp without time zone NOT NULL,
    cl double precision,
    strategy character varying(50) COLLATE pg.pg_catalog."default" NOT NULL
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.block_code3_deatil
    OWNER to mini;

GRANT ALL ON TABLE public.block_code3_deatil TO mini;