excl <- fread("dados/CAGEDEXC202403.txt", encoding = "UTF-8")
fora <- fread("dados/CAGEDFOR202403.txt", encoding = "UTF-8")
mova <- fread("dados/CAGEDMOV202403.txt", encoding = "UTF-8")
mova1 <- fread("dados/CAGEDMOV202001.txt", encoding = "UTF-8")

mova |> count(saldomovimentação)