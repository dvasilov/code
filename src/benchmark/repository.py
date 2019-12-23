from allocation import model


class BatchRepository:

    def __init__(self, session):
        super().__init__()
        self.session = session

    def add(self, batch):
        self.session.add(batch)

    def get(self, ref):
        return self.session.query(model.Batch).filter_by(reference=ref).first()


class BatchWithForUpdateRepository:

    def __init__(self, session):
        super().__init__()
        self.session = session

    def add(self, batch):
        self.session.add(batch)

    def get(self, ref):
        return self.session.query(model.Batch).filter_by(reference=ref).with_for_update().first()
