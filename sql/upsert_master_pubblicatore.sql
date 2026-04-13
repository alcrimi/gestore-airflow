INSERT INTO metadata.master_pubblicatore
    (progetto_output, dataset_output, entity_output, sistema_output, entity_input, sistema_input)
VALUES
    (:progetto_output, :dataset_output, :entity_output, :sistema_output, :entity_input, :sistema_input)
ON CONFLICT (progetto_output, dataset_output, entity_output, sistema_output, entity_input)
DO UPDATE SET sistema_input = EXCLUDED.sistema_input;
