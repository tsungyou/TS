-- Table: public.stock_1m_price

-- DROP TABLE IF EXISTS public.stock_1m_price;

CREATE TABLE IF NOT EXISTS public.stock_1m_price
(
    da timestamp without time zone NOT NULL,
    code character varying(25) COLLATE pg_catalog."default" NOT NULL,
    cl double precision,
    hi double precision,
    lo double precision,
    op double precision,
    vol bigint,
    adj double precision,
    CONSTRAINT blp_stockprice_pkey PRIMARY KEY (da, code)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.stock_1m_price
    OWNER to mini;

REVOKE ALL ON TABLE public.stock_1m_price FROM PUBLIC;

GRANT DELETE, UPDATE, INSERT ON TABLE public.stock_1m_price TO mini;

GRANT INSERT, DELETE, UPDATE ON TABLE public.stock_1m_price TO mini;

GRANT ALL ON TABLE public.stock_1m_price TO mini;

GRANT SELECT ON TABLE public.stock_1m_price TO PUBLIC;
-- Index: idx_stock_price_da

-- DROP INDEX IF EXISTS public.idx_stock_price_da;

CREATE INDEX IF NOT EXISTS idx_stock_price_da
    ON public.stock_1m_price USING btree
    (da ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: name

-- DROP INDEX IF EXISTS public.name;

CREATE INDEX IF NOT EXISTS name
    ON public.stock_1m_price USING btree
    (da ASC NULLS LAST, code COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;