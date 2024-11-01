-- Table: public.us_stock_price

DROP TABLE IF EXISTS public.us_stock_price;

CREATE TABLE IF NOT EXISTS public.us_stock_price
(
    da timestamp without time zone NOT NULL,
    code character varying(25) COLLATE pg_catalog."default" NOT NULL,
    cl double precision,
    CONSTRAINT blp_stockprice_pkey_1m PRIMARY KEY (da, code)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.us_stock_price
    OWNER to postgres;

REVOKE ALL ON TABLE public.us_stock_price FROM PUBLIC;

GRANT DELETE, UPDATE, INSERT ON TABLE public.us_stock_price TO postgres;

GRANT INSERT, DELETE, UPDATE ON TABLE public.us_stock_price TO postgres;

GRANT ALL ON TABLE public.us_stock_price TO postgres;

GRANT SELECT ON TABLE public.us_stock_price TO PUBLIC;
-- Index: idx_stock_price_da

-- DROP INDEX IF EXISTS public.idx_stock_price_da;

CREATE INDEX IF NOT EXISTS idx_stock_price_da
    ON public.us_stock_price USING btree
    (da ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: name

-- DROP INDEX IF EXISTS public.name;

CREATE INDEX IF NOT EXISTS name
    ON public.us_stock_price USING btree
    (da ASC NULLS LAST, code COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;