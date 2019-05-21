--
-- PostgreSQL database dump
--

-- Dumped from database version 9.5.3
-- Dumped by pg_dump version 9.5.1

-- Started on 2016-08-07 13:08:32

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

SET search_path = test, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- TOC entry 184 (class 1259 OID 16439)
-- Name: po_line; Type: TABLE; Schema: test; Owner: postgres
--

create schema test;

CREATE TABLE test.po_line (
    id integer NOT NULL,
    jsonb jsonb
);


ALTER TABLE test.po_line OWNER TO username;

--
-- TOC entry 183 (class 1259 OID 16437)
-- Name: po_line__id_seq; Type: SEQUENCE; Schema: test; Owner: postgres
--

CREATE SEQUENCE po_line__id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE po_line__id_seq OWNER TO username;

--
-- TOC entry 2132 (class 0 OID 0)
-- Dependencies: 183
-- Name: po_line__id_seq; Type: SEQUENCE OWNED BY; Schema: test; Owner: postgres
--

ALTER SEQUENCE po_line__id_seq OWNED BY test.po_line.id;


--
-- TOC entry 2009 (class 2604 OID 16442)
-- Name: id; Type: DEFAULT; Schema: test; Owner: postgres
--

ALTER TABLE ONLY test.po_line ALTER COLUMN _id SET DEFAULT nextval('po_line__id_seq'::regclass);


--
-- TOC entry 2127 (class 0 OID 16439)
-- Dependencies: 184
-- Data for Name: po_line; Type: TABLE DATA; Schema: test; Owner: postgres
--

COPY test.po_line (id, jsonb) FROM stdin;
1	{"note": [], "rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "USD"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "SENT"}, "renewal_period": "", "access_provider": "", "fund_distribution": [], "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "vendor_account_CODE": "YBP_CODE", "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
2	{"note": [], "rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "USD"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "PENDING"}, "renewal_period": "", "access_provider": "", "fund_distribution": [], "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "vendor_account_CODE": "YBP_CODE", "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
4	{"note": [], "rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "USD"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "REVIEW"}, "renewal_period": "", "access_provider": "", "fund_distribution": [], "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "vendor_account_CODE": "YBP_CODE", "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
5	{"note": [], "rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "USD"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "REVIEW2"}, "renewal_period": "", "access_provider": "", "fund_distribution": [], "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "vendor_account_CODE": "YBP_CODE", "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
6	{"note": [], "rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "USD"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "REVIEW2"}, "renewal_period": "", "access_provider": "", "fund_distribution": [], "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "vendor_account_CODE": "YBP_CODE", "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
7	{"note": [], "rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "USD"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "REVIEW2"}, "renewal_period": "", "access_provider": "", "fund_distribution": [], "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "vendor_account_CODE": "YBP_CODE", "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
8	{"note": [], "rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "USD"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "REVIEW2"}, "renewal_period": "", "access_provider": "", "fund_distribution": [], "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "vendor_account_CODE": "YBP_CODE", "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
9	{"note": [], "rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "USD"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "REVIEW2"}, "renewal_period": "", "access_provider": "", "fund_distribution": [], "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "vendor_account_CODE": "YBP_CODE", "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
10	{"note": [], "rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "USD"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "REVIEW2"}, "renewal_period": "", "access_provider": "", "fund_distribution": [], "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "vendor_account_CODE": "YBP_CODE", "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
16	{"note": [], "rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "USD"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "SOMETHING_NEW2"}, "renewal_period": "", "access_provider": "", "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "fund_distributions": [{"amount": {"sum": 123.5, "currency": "USD"}, "fund_code": "12345"}], "vendor_account_CODE": "YBP_CODE", "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
19	{"note": [], "rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "USD"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "SOMETHING_NEW4"}, "renewal_period": "", "access_provider": "", "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "fund_distributions": [{"amount": {"sum": 123.5, "currency": "USD"}, "fund_code": "12345"}], "vendor_account_CODE": "YBP_CODE", "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
21	{"note": [], "rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "USD"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "SOMETHING_NEW4"}, "renewal_period": "", "access_provider": "", "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "fund_distributions": [{"amount": {"sum": 123.5, "currency": "USD"}, "fund_code": "12345"}], "vendor_account_CODE": "YBP_CODE", "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
20	{"note": [], "rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "USD"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "SOMETHING_NEW3"}, "renewal_period": "", "access_provider": "", "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "fund_distributions": [{"amount": {"sum": 123.5, "currency": "USD"}, "fund_code": "12345"}], "vendor_account_CODE": "YBP_CODE", "po_line_status->value": {"value": "SOMETHING_NEW6"}, "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
18	{"note": [], "rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "USD"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "SOMETHING_NEW4"}, "renewal_period": "", "access_provider": "", "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "fund_distributions": [{"amount": {"sum": 123.5, "currency": "USD"}, "fund_code": "12345"}], "vendor_account_CODE": "YBP_CODE", "po_line_status->value": "SOMETHING_NEW6", "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
17	{"note": [], "rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "USD"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "SOMETHING_NEW5"}, "renewal_period": "", "access_provider": "", "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "fund_distributions": [{"amount": {"sum": 123.5, "currency": "USD"}, "fund_code": "12345"}], "vendor_account_CODE": "YBP_CODE", "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
15	{"rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "SHEKEL"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "SOMETHING_NEWERest123"}, "renewal_period": "", "access_provider": "", "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "fund_distributions": [{"amount": {"sum": 123.5, "currency": "SHEKEL"}}], "vendor_account_CODE": "YBP_CODE", "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
14	{"rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "SHEKEL"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "SOMETHING_NEWERest123"}, "renewal_period": "", "access_provider": "", "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "fund_distributions": [{"amount": {"sum": 123.5, "currency": "SHEKEL"}}], "vendor_account_CODE": "YBP_CODE", "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
13	{"note": ["a", "b", "c"], "rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "USD"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "IN_REVIEW"}, "renewal_period": "", "access_provider": "", "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "fund_distributions": [{"amount": {"sum": 123.5, "currency": "USD"}, "fund_code": "12345"}], "vendor_account_CODE": "YBP_CODE", "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
11	{"note": ["a"], "rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "USD"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "REVIEW2"}, "renewal_period": "", "access_provider": "", "fund_distribution": [], "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "vendor_account_CODE": "YBP_CODE", "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
22	{"note": [], "rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "USD"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "SOMETHING_NEW3"}, "renewal_period": "", "access_provider": "", "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "fund_distributions": [{"amount": {"sum": 133.5, "currency": "USD"}, "fund_code": "12345"}], "vendor_account_CODE": "YBP_CODE", "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
23	{"note": [], "rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "USD"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "SOMETHING_NEW3"}, "renewal_period": "", "access_provider": "", "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "fund_distributions": [{"amount": {"sum": 153.5, "currency": "USD"}, "fund_code": "12345"}], "vendor_account_CODE": "YBP_CODE", "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
24	{"note": [], "rush": false, "type": {"desc": "", "value": "PRINT_ONETIME"}, "owner": {"desc": "Math Library", "value": "MITLIBMATH"}, "price": {"sum": "150.0", "po_currency": {"desc": "US Dollar", "value": "USD"}}, "vendor": {"desc": "", "value": "YBP"}, "location": [], "ebook_url": "", "po_number": "0987654321", "source_type": "API", "update_date": "", "created_date": "", "renewal_date": "", "material_type": "BOOK", "po_line_status": {"desc": "sent to vendor", "value": "SOMETHING_NEW3"}, "renewal_period": "", "access_provider": "", "invoice_reference": "", "resource_metadata": "/abc/v1/bibs/99113721800121", "fund_distributions": [{"amount": {"sum": 133.5, "currency": "USD"}, "fund_code": "12345"}], "vendor_account_CODE": "YBP_CODE", "block_alert_on_po_line": [{"desc": "Fund is missing", "value": "FUNDMISS"}], "acquisition_method_CODE": {"desc": "Purchased at Vendor System", "value": "VENDOR_SYSTEM"}, "vendor_reference_number": "ybp-1234567890"}
\.


--
-- TOC entry 2133 (class 0 OID 0)
-- Dependencies: 183
-- Name: po_line__id_seq; Type: SEQUENCE SET; Schema: test; Owner: postgres
--

SELECT pg_catalog.setval('po_line__id_seq', 24, true);


--
-- TOC entry 2011 (class 2606 OID 16456)
-- Name: id; Type: CONSTRAINT; Schema: test; Owner: postgres
--

ALTER TABLE ONLY test.po_line
    ADD CONSTRAINT id PRIMARY KEY (id);


-- Completed on 2016-08-07 13:08:33

--
-- PostgreSQL database dump complete
--

