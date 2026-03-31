-- Migración: quitar num_cuotas de splits
-- Los splits aplican a toda la compra (igual que clasificaciones), no por cuota individual.
-- Ejecutar en Supabase > SQL Editor.

ALTER TABLE splits DROP COLUMN IF EXISTS num_cuotas;
