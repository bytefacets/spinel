# Vaadin

This module provides components which simplify exposing transform outputs in [Vaadin](https://vaadin.com).

## ValueProvider
[FieldValueProvider](./src/main/java/com/bytefacets/spinel/vaadin/data/FieldValueProvider.java)
allows you to expose data in a Field to Vaadin.

[FormattedValueProvider](./src/main/java/com/bytefacets/spinel/vaadin/data/FormattedValueProvider.java)
exposes data in the field, but formats it to a String according to the SchemaField's metadata.

## Renderers
The [RendererFactory](./src/main/java/com/bytefacets/spinel/vaadin/data/RendererFactory.java)
has methods to facilitate TextRenderer creation and LitRenderer creation bound to a field.

## DataProvider
The [TransformDataProvider](./src/main/java/com/bytefacets/spinel/vaadin/data/TransformDataProvider.java)
is an implementation that adapts the TransformOutput to the Vaadin DataProvider interface
allowing it to be used in components like Grids and ComboBoxes.

## GridAdapter
The [GridAdapter](./src/main/java/com/bytefacets/spinel/vaadin/grid/GridAdapter.java)
adapters a TransformOutput to a Vaadin Grid component.

### Example
Below is an example of a GridAdapter used in a Div. This example assumes that OutputRegistry is
exposed, perhaps as a Spring Bean, and has in it a TransformOutput that is being updated with the data.
In this case, I had another server running which was getting the data, and this server which has 
the UI was subscribing to that data, e.g. Order Feed -> Data Server -> View Server (with Vaadin).

![Vaadin Grid Example](https://bytefacets.github.io/site/assets/images/vaadin-grid-example.gif)

```java
@PageTitle("Orders")
@Route("")
@Menu(order = 0, icon = LineAwesomeIconUrl.FILTER_SOLID)
@PermitAll
@Uses(Icon.class)
public class Orders extends Div {
    private final GridAdapter gridAdapter;
    private final DefaultSubscriptionProvider provider;
    private final Grid<TransformRow> grid;
    private final ConnectedUserSession userSession;

    public Orders(
            final OutputRegistry outputRegistry,
            final EventLoop eventLoop,
            final AuthenticationContext authenticationContext) {
        this.provider = defaultSubscriptionProvider(outputRegistry);
        this.userSession = new ConnectedUserSession(authenticationContext);
        final SubscriptionContainer sub =
                provider.getSubscription(userSession, config(), List.of());
        this.gridAdapter =
                GridAdapterBuilder.gridAdapter(sub, eventLoop)
                        .fieldOrder("OrderId", "Symbol", "OpenQuantity", "Price", "LastPrice")
                        .customizeColumn(
                                "OpenQuantity",
                                (column, schemaField, valueProvider) -> column.setRenderer(changedRenderer(schemaField, "changed")))
                        .build();
        gridAdapter.refreshInterval(UI.getCurrent(), Duration.ofMillis(250));
        addDetachListener(this::disconnect);
        this.grid = gridAdapter.grid();
        setSizeFull();
        final VerticalLayout layout =  new VerticalLayout(grid);
        layout.setSizeFull();
        layout.setPadding(false);
        layout.setSpacing(false);
        add(layout);
    }

    private void disconnect(final DetachEvent event) {
        gridAdapter.disconnect();
    }

    private SubscriptionConfig config() {
        return SubscriptionConfig.subscriptionConfig("order-view")
                .setFields(List.of("OrderId", "Symbol", "OpenQuantity", "Price", "LastPrice"))
                .defaultAll(true)
                .build();
    }

    private void refreshGrid() {
        grid.getDataProvider().refreshAll();
    }
}
```

The architecture for this example, just to give you an idea looks like this:

![Vaadin Architecture](https://bytefacets.github.io/site/assets/images/vaadin-arch.jpg)
