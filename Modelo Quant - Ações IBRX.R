# ============================
# IBRX-100 (ações BR) + ÍNDICE OFICIAL IBRX-100 (B3)
# ============================

# ---------- Pacotes ----------
req <- c("rb3","dplyr","tidyr","stringr","lubridate","yfR","xts","purrr",
         "tibble","quantmod","PerformanceAnalytics")
to_install <- setdiff(req, rownames(installed.packages()))
if (length(to_install)) install.packages(to_install, dependencies = TRUE)

library(rb3)
library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)
library(yfR)
library(xts)
library(purrr)
library(tibble)
library(quantmod)
library(PerformanceAnalytics)

# --------------------------------
# 1) Parâmetros gerais do estudo
# --------------------------------
start_date <- as.Date("2016-01-01")  # início do histórico
end_date   <- Sys.Date()             # fim do histórico
Rf_anual   <- 0.0425                 # taxa livre de risco anual (4,25% a.a.)
CDS_anual  <- 0.0136                 # CDS BR (1,36% a.a.) — ADITIVO no CAPM

# --------------------------------
# 2) Carteira teórica atual do IBRX-100 (B3)
#    - Baixa a carteira oficial e normaliza o nome da coluna de data
# --------------------------------
message("Baixando carteira teórica atual do IBRX-100 na B3...")
fetch_marketdata("b3-indexes-current-portfolio", index = "IBXX", throttle = TRUE)

df_ibrx_raw <- indexes_current_portfolio_get() %>%
  filter(index == "IBXX") %>%
  collect()

# Compatibilidade: encontra a coluna de data que a versão do rb3 estiver usando
ref_col <- intersect(c("refdate","reference_date","portfolio_date","date"), names(df_ibrx_raw))
if (length(ref_col) == 0) stop("Não encontrei coluna de data na carteira do IBRX-100.")

df_ibrx <- df_ibrx_raw %>%
  rename(refdate = !!ref_col[1]) %>%
  select(any_of(c("refdate","index","symbol","weight","sector")))

# Mantém apenas códigos locais no padrão B3 (PETR4, VALE3, BBDC4, BIDI11, etc.)
tickers_b3 <- df_ibrx %>%
  mutate(symbol = toupper(symbol)) %>%
  filter(str_detect(symbol, "^[A-Z]{4}[0-9]{1,2}$")) %>%
  pull(symbol) %>%
  unique() %>%
  sort()

# --------------------------------
# 3) Históricos de preços das ações (Yahoo via yfR)
#    - Baixa apenas tickers com ≥ 75% de dados válidos no período
#    - Organiza em formato wide (datas x tickers)
# --------------------------------
tickers_yahoo <- paste0(tickers_b3, ".SA")

message("Baixando históricos das ações do IBRX-100 via yfR (Yahoo)...")
yf_prices <- yf_get(
  tickers          = tickers_yahoo,
  first_date       = start_date,
  last_date        = end_date,
  freq_data        = "daily",
  do_complete_data = TRUE,   # alinha todos os tickers na mesma grade de datas
  thresh_bad_data  = 0.75,   # exige pelo menos 75% de observações válidas
  cache_folder     = file.path(tempdir(), "yfR_cache")
)

stock_prices_df <- yf_prices %>%
  select(ref_date, ticker, price_adjusted) %>%
  mutate(ticker = str_remove(ticker, "\\.SA$")) %>%  # tira o sufixo .SA
  distinct() %>%
  pivot_wider(names_from = ticker, values_from = price_adjusted) %>%
  arrange(ref_date)

# --------------------------------
# 4) Série oficial do índice IBRX-100 (B3)
#    - Usa rb3::index_get() quando disponível; senão, fallback ano a ano
# --------------------------------
get_ibrx_official <- function(first_date, last_date) {
  if ("index_get" %in% getNamespaceExports("rb3")) {
    return(rb3::index_get("IBXX", first_date = first_date, last_date = last_date) %>% arrange(refdate))
  }
  yrs <- seq(year(first_date), year(last_date))
  for (yy in yrs) {
    rb3::fetch_marketdata("b3-indexes-historical-data", index = "IBXX", year = yy, throttle = TRUE)
  }
  df_raw <- rb3::indexes_historical_data_get() %>% collect()
  if ("index_name" %in% names(df_raw)) df_raw <- df_raw %>% filter(index_name == "IBXX")
  else if ("index" %in% names(df_raw)) df_raw <- df_raw %>% filter(index == "IBXX")
  
  value_col <- dplyr::case_when(
    "value" %in% names(df_raw) ~ "value",
    "index_value" %in% names(df_raw) ~ "index_value",
    "close" %in% names(df_raw) ~ "close",
    TRUE ~ NA_character_
  )
  if (is.na(value_col)) stop("Não encontrei a coluna de valor do índice no dataset histórico.")
  df_raw %>%
    mutate(refdate = as.Date(refdate)) %>%
    filter(refdate >= first_date, refdate <= last_date) %>%
    arrange(refdate) %>%
    select(refdate, value = all_of(value_col)) %>%
    distinct(refdate, .keep_all = TRUE)
}

ibrx_df  <- get_ibrx_official(start_date, end_date)
ibrx_xts <- xts::xts(ibrx_df$value, order.by = ibrx_df$refdate)
colnames(ibrx_xts) <- "IBRX"

# ==================================================
# 6) Retornos mensais (por TICKER) e do ÍNDICE
#    - Constrói preços mensais e retorna LOG por ticker
#    - Mantém NAs por coluna (não dá na.omit no painel inteiro)
# ==================================================
prices_xts <- xts::xts(stock_prices_df[,-1], order.by = stock_prices_df$ref_date)

monthly_prices_list <- lapply(colnames(prices_xts), function(ticker) {
  x <- prices_xts[, ticker, drop = FALSE]
  colnames(x) <- "Adjusted"
  Cl(to.monthly(x, indexAt = "lastof", drop.time = TRUE))
})
monthly_prices_xts <- do.call(merge, monthly_prices_list)
colnames(monthly_prices_xts) <- colnames(prices_xts)

# Índice IBRX em base mensal
monthly_ibrx_prices  <- Cl(to.monthly(ibrx_xts, indexAt = "lastof", drop.time = TRUE))

# Retornos mensais LOG por ticker (mantém NAs por coluna)
ret_all <- Return.calculate(monthly_prices_xts, method = "log")

# Retorno mensal LOG do mercado (IBRX) – aqui podemos remover NAs do índice
monthly_market_returns <- na.omit(Return.calculate(monthly_ibrx_prices, method = "log"))
index(ret_all) <- as.Date(index(ret_all))
index(monthly_market_returns) <- as.Date(index(monthly_market_returns))
colnames(monthly_market_returns) <- "MKT"

# ============================================
# 7) CAPM: Betas por ticker, Rm anual (IBRX), Vol e Sharpe
#    - RF mensal em LOG para excesso de retorno consistente com ret_all
#    - Beta/Vol calculados por ticker (pairwise), não no painel todo
#    - Filtros de qualidade: mínimo de 12 meses por métrica
#    - CDS ANUAL ADITIVO no CAPM (CDS_anual)
# ============================================
Rf_mensal     <- (1 + Rf_anual)^(1/12) - 1
Rf_mensal_log <- log(1 + Rf_mensal)

min_obs_beta <- 12  # mínimo de meses para estimar beta
min_obs_vol  <- 12  # mínimo de meses para estimar volatilidade

# Beta por ticker
betas <- sapply(colnames(ret_all), function(tk) {
  par <- merge(ret_all[, tk, drop = TRUE], monthly_market_returns[, "MKT", drop = TRUE], join = "inner")
  Ri  <- as.numeric(par[, 1]) - Rf_mensal_log
  Rm  <- as.numeric(par[, 2]) - Rf_mensal_log
  ok  <- is.finite(Ri) & is.finite(Rm)
  if (sum(ok) < min_obs_beta) return(NA_real_)
  cov(Ri[ok], Rm[ok], use = "complete.obs") / var(Rm[ok], na.rm = TRUE)
})

# Rm anual (a partir do IBRX no período em LOG → simples)
Rm_log_mensal_med <- mean(as.numeric(coredata(monthly_market_returns)), na.rm = TRUE)
Rm_anual <- exp(12 * Rm_log_mensal_med) - 1
cat(sprintf("Rm anual (IBRX): %.2f%%\n", 100*Rm_anual))

# Retorno esperado anual (CAPM) por ticker — ADITIVO DO CDS
# (mesma lógica, apenas somando CDS_anual ao resultado do CAPM)
expected_returns <- Rf_anual + betas * (Rm_anual - Rf_anual) + CDS_anual

# Volatilidade anual por ticker
months_vol <- colSums(!is.na(ret_all))
vol_anual <- sapply(colnames(ret_all), function(tk) {
  x <- as.numeric(ret_all[, tk])
  if (sum(is.finite(x)) < min_obs_vol) return(NA_real_)
  sd(x, na.rm = TRUE) * sqrt(12)
})

# Sharpe anual (ex-ante): (E[Ri]-Rf)/σ_i — mesma lógica
sharpe_ratios <- (expected_returns - Rf_anual) / vol_anual

# --------------------------------
# Diagnóstico rápido das amostras por métrica
# --------------------------------
months_beta <- sapply(colnames(ret_all), function(tk) {
  nrow(merge(ret_all[, tk, drop = FALSE],
             monthly_market_returns[, "MKT", drop = FALSE],
             join = "inner"))
})
message(sprintf("Tickers com beta válido (>= %d meses): %d", min_obs_beta, sum(!is.na(betas))))
message(sprintf("Tickers com vol válida  (>= %d meses): %d", min_obs_vol,  sum(!is.na(vol_anual))))

# ============================================
# 8) ALFA
#    - CAGR anualizado do preço de 2016→hoje por ticker, menos CAPM esperado
#    - Observação: aqui a janela é global; não há filtro mínimo por ticker
# ============================================
precos_iniciais <- stock_prices_df %>% slice(1) %>% select(-ref_date) %>% unlist(use.names = TRUE)
precos_finais   <- stock_prices_df %>% slice_tail(n = 1) %>% select(-ref_date) %>% unlist(use.names = TRUE)

retorno_acumulado  <- precos_finais / precos_iniciais - 1
n_years <- as.numeric(difftime(max(stock_prices_df$ref_date, na.rm = TRUE),
                               min(stock_prices_df$ref_date, na.rm = TRUE),
                               units = "days")) / 365.25
retorno_anualizado <- (1 + retorno_acumulado)^(1 / n_years) - 1

alpha_capm <- retorno_anualizado - expected_returns
tabela_alphas <- data.frame(
  ticker = names(alpha_capm),
  alpha_capm = alpha_capm
) %>% arrange(desc(alpha_capm))

# ============================================
# 9) Score combinado — Normalização min–max robusta
#    - Só entra quem tem Sharpe e Alpha válidos
# ============================================
min_max_norm <- function(x) {
  rng <- range(x, na.rm = TRUE)
  if (!is.finite(diff(rng)) || diff(rng) == 0) return(rep(0.5, length(x)))
  (x - rng[1]) / diff(rng)
}

valid_tickers <- intersect(names(sharpe_ratios[!is.na(sharpe_ratios)]),
                           names(alpha_capm[!is.na(alpha_capm)]))

df_scores <- data.frame(
  ticker = valid_tickers,
  sharpe = as.numeric(sharpe_ratios[valid_tickers]),
  alpha  = as.numeric(alpha_capm[valid_tickers])
)
df_scores$sharpe_norm <- min_max_norm(df_scores$sharpe)
df_scores$alpha_norm  <- min_max_norm(df_scores$alpha)
df_scores$score_combinado <- rowMeans(df_scores[, c("sharpe_norm","alpha_norm")])

# ---------- Saídas usuais ----------
top10_betas <- sort(betas, decreasing = TRUE)[1:10]; print(top10_betas)

tabela_retornos <- data.frame(ticker = names(expected_returns),
                              retorno_esperado_anual = as.numeric(expected_returns)) %>%
  arrange(desc(retorno_esperado_anual))
print(head(tabela_retornos, 10))

tabela_sharpe <- data.frame(ticker = names(sharpe_ratios),
                            sharpe = as.numeric(sharpe_ratios)) %>%
  arrange(desc(sharpe))
print(head(tabela_sharpe, 10))

print(head(tabela_alphas, 10))

top10_combinado <- df_scores %>% arrange(desc(score_combinado)) %>% slice_head(n = 10)
top20_combinado <- df_scores %>% arrange(desc(score_combinado)) %>% slice_head(n = 20)
print(top10_combinado); print(top20_combinado)

# ---------- Tabela Top 20 ----------
library(gt)
library(dplyr)
library(scales)

tbl_top20 <- top20_combinado %>%
  arrange(desc(score_combinado)) %>%
  mutate(Rank = row_number()) %>%
  select(
    Rank, ticker, sharpe, alpha,
    sharpe_norm, alpha_norm, score_combinado
  )

tab_top20 <- tbl_top20 %>%
  gt() %>%
  tab_header(
    title = md("**Top 20 Ações — IBrX-100 (Score Combinado)**"),
    subtitle = "Sharpe e Alpha com normalização min–max (0–1)"
  ) %>%
  cols_label(
    ticker = "Ticker",
    sharpe = "Sharpe",
    alpha = "Alpha (anual)",
    sharpe_norm = "Sharpe (Norm.)",
    alpha_norm = "Alpha (Norm.)",
    score_combinado = "Score"
  ) %>%
  tab_spanner(
    label = "Normalizados (0–1)",
    columns = c(sharpe_norm, alpha_norm)
  ) %>%
  fmt_number(columns = c(sharpe, sharpe_norm, alpha_norm, score_combinado),
             decimals = 3) %>%
  fmt_percent(columns = alpha, decimals = 1) %>%
  cols_align(columns = c(Rank, ticker), align = "center") %>%
  opt_row_striping() %>%
  tab_style(
    style = list(cell_text(weight = "bold")),
    locations = cells_column_labels()
  ) %>%
  # destaque para o 1º lugar (amarelo suave)
  tab_style(
    style = list(
      cell_fill(color = "#FFF3B0"),
      cell_text(weight = "bold")
    ),
    locations = cells_body(rows = Rank == 1)
  ) %>%
  # cores verde → amarelo → vermelho
  data_color(
    columns = c(sharpe_norm, alpha_norm, score_combinado),
    colors = col_numeric(
      palette = c("red", "yellow", "green"),
      domain = NULL
    )
  ) %>%
  tab_options(
    table.font.size = px(12),
    heading.background.color = "white",
    column_labels.background.color = "#F2F2F2",
    data_row.padding = px(6)
  ) %>%
  tab_source_note(md("*Score = média de Sharpe (Norm.) e Alpha (Norm.).*"))

tab_top20
