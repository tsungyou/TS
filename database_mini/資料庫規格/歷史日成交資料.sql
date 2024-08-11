-- Table: public.price

-- DROP TABLE IF EXISTS public.price;

CREATE TABLE IF NOT EXISTS public.price
(
    da timestamp without time zone NOT NULL,
    vol_share bigint,
    vol bigint,
    cl double precision,
    hi double precision,
    lo double precision,
    op double precision,
    code character varying(50) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT price_backup_pkey PRIMARY KEY (code, da)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.price
    OWNER to mini;

GRANT ALL ON TABLE public.price TO mini;
-- Index: idx_price_backup_code

-- DROP INDEX IF EXISTS public.idx_price_backup_code;

CREATE INDEX IF NOT EXISTS idx_price_backup_code
    ON public.price USING btree
    (code COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_price_code

-- DROP INDEX IF EXISTS public.idx_price_code;

CREATE INDEX IF NOT EXISTS idx_price_code
    ON public.price USING btree
    (code COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_price_da

-- DROP INDEX IF EXISTS public.idx_price_da;

CREATE INDEX IF NOT EXISTS idx_price_da
    ON public.price USING btree
    (da ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_price_future_backup_code

-- DROP INDEX IF EXISTS public.idx_price_future_backup_code;

CREATE INDEX IF NOT EXISTS idx_price_future_backup_code
    ON public.price USING btree
    (code COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: price_backup_index

-- DROP INDEX IF EXISTS public.price_backup_index;

CREATE INDEX IF NOT EXISTS price_backup_index
    ON public.price USING btree
    (da ASC NULLS LAST, code COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: price_future_backup_index

-- DROP INDEX IF EXISTS public.price_future_backup_index;

CREATE INDEX IF NOT EXISTS price_future_backup_index
    ON public.price USING btree
    (da ASC NULLS LAST, code COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;