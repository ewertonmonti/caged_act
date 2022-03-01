################################################################################
###          EMPREGO NAS ATIVIDADES CARACTERÍSTICAS DO TURISMO (ACTs)        ###
################################################################################

# Elaboração de banco de dados com o saldo de empregos nas ACTs
# a partir dos dados novo CAGED (metodologia atualizada em Outubro/2021)

# Fazer download dos arquivos do CAGED disponíveis em ftp.mtps.gov.br/pdet/microdados/NOVO CAGED
# Descompactar arquivos em uma pasta
# Informar caminho da pasta
pasta.txt <- "C:/R/caged/" # substituir pelo caminho da pasta no computador (manter a barra no final do caminho)
pasta.txt <- "M:/Dados/CAGED/" # substituir pelo caminho da pasta no computador

# Bibliotecas
library(data.table)
library(tidyverse)
library(janitor) # para limpar nomes
library(lubridate) # para parse datas

# Lista de ACTs segundo o IPEA
# Fonte: https://www.ipea.gov.br/extrator/arquivos/160204_caracterizacao_br_re.pdf
act_lista <- fread("acts_ipea.csv")

# Divisão territorial brasileira
# Fonte: https://www.ibge.gov.br/geociencias/organizacao-do-territorio/estrutura-territorial/23701-divisao-territorial-brasileira.html
dtb <- fread("dtb_2021.csv")

uf_regiao <- fread("uf_regiao.csv") # Nomes de UF e regiões brasileiras

files <- list.files(path = pasta.txt, pattern="CAGED.*.txt") # Listar arquivos

caged_file <- list() # Criar lista vazia para receber dados

# Ler e selecionar dados dos múltiplos arquivos
for (i in seq_along(files)) {
  caged_file[[i]] <- fread(file = paste0(pasta.txt, files[i]), encoding = "UTF-8", select = c("competênciamov", "município", "subclasse", "saldomovimentação")) # Ler dados de cada arquivo
  caged_file[[i]] <- caged_file[[i]][caged_file[[i]]$subclasse %in% act_lista$subclasse] # Selecionar apenas linhas de ACTs
  print(paste0("Arquivos processados: ",i)) # Acompanhar processamento
}

caged <- bind_rows(caged_file) # Empilhar dados
rm(caged_file) # Deletar lista com dados que foram empilhados

caged <- clean_names(caged) # Limpar nomes das variáveis (bancos originais têm falhas)

caged <- left_join(caged, act_lista[, .(subclasse, act)], by="subclasse") # Identificar ACT

caged <- caged[, .(saldo = sum(saldomovimentacao)), by = .(competenciamov, municipio, act)] # Resumir saldo de empregos por mês, município e ACT

caged$uf_codigo <- floor(caged$municipio/10000) # Criar variável UF

dtb$municipio <- floor(as.numeric(dtb$`Código Município Completo`)/10)  # Recodificar código do município em DTB para formato 6 dígitos

caged <- left_join(caged, dtb[, .(municipio, Nome_Município, `Nome Região Geográfica Imediata`)],
                   by = "municipio") # Identificar nome do município e da região geográfica imediata

caged <- left_join(caged, uf_regiao, by = "uf_codigo") # Identificar nome da UF e da região

caged$mes <- ym(caged$competenciamov) # Transformar competenciamov para o formato data

caged <- caged[, -c("competenciamov", "municipio", "uf_codigo")] # Excluir colunas desnecessárias

write.csv(caged, "caged_act.csv", fileEncoding = "UTF-8") # Salvar arquivo csv
