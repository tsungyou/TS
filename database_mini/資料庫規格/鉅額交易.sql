['code', 'codename', 'type_', 'cl', 'vol_share', 'vol', 'da']


-- Table: public.maincode

-- DROP TABLE IF EXISTS public.maincode;

CREATE TABLE IF NOT EXISTS public.block_trade
(
    code character varying(50) COLLATE pg_catalog."default" NOT NULL,
    cname character varying(50) COLLATE pg_catalog."default" NOT NULL,
    type_ character varying(50) COLLATE pg_catalog."default" NOT NULL,
    cl double precision,
    vol_share bigint,
    vol bigint,
    da timestamp without time zone NOT NULL
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.maincode
    OWNER to mini;

GRANT ALL ON TABLE public.maincode TO mini;


tw=# \d block_trade;
                        Table "public.block_trade"
  Column   |            Type             | Collation | Nullable | Default 
-----------+-----------------------------+-----------+----------+---------
 code      | character varying(50)       |           | not null | 
 cname     | character varying(50)       |           | not null | 
 type_     | character varying(50)       |           | not null | 
 cl        | double precision            |           |          | 
 vol_share | bigint                      |           |          | 
 vol       | bigint                      |           |          | 
 da        | timestamp without time zone |           | not null | 